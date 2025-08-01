/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.search.lookup.SourceFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A (mostly) immutable snapshot of the current mapping of an index with
 * access to everything we need for the search phase.
 */
public final class MappingLookup {
    /**
     * Key for the lookup to be used in caches.
     */
    public static class CacheKey {
        private CacheKey() {}
    }

    /**
     * A lookup representing an empty mapping. It can be used to look up fields, although it won't hold any, but it does not
     * hold a valid {@link DocumentParser}, {@link IndexSettings} or {@link IndexAnalyzers}.
     */
    public static final MappingLookup EMPTY = fromMappers(Mapping.EMPTY, List.of(), List.of());

    private final CacheKey cacheKey = new CacheKey();

    /** Full field name to mapper */
    private final Map<String, Mapper> fieldMappers;
    private final Map<String, ObjectMapper> objectMappers;
    private final Map<String, InferenceFieldMetadata> inferenceFields;
    private final Set<String> syntheticVectorFields;
    private final int runtimeFieldMappersCount;
    private final NestedLookup nestedLookup;
    private final FieldTypeLookup fieldTypeLookup;
    private final FieldTypeLookup indexTimeLookup;  // for index-time scripts, a lookup that does not include runtime fields
    private final Map<String, NamedAnalyzer> indexAnalyzersMap;
    private final List<FieldMapper> indexTimeScriptMappers;
    private final Mapping mapping;
    private final int totalFieldsCount;

    /**
     * Creates a new {@link MappingLookup} instance by parsing the provided mapping and extracting its field definitions.
     *
     * @param mapping the mapping source
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMapping(Mapping mapping) {
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        List<PassThroughObjectMapper> newPassThroughMappers = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : mapping.getSortedMetadataMappers()) {
            if (metadataMapper != null) {
                newFieldMappers.add(metadataMapper);
            }
        }
        for (Mapper child : mapping.getRoot()) {
            collect(child, newObjectMappers, newFieldMappers, newFieldAliasMappers, newPassThroughMappers);
        }
        return new MappingLookup(mapping, newFieldMappers, newObjectMappers, newFieldAliasMappers, newPassThroughMappers);
    }

    private static void collect(
        Mapper mapper,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldMapper> fieldMappers,
        Collection<FieldAliasMapper> fieldAliasMappers,
        Collection<PassThroughObjectMapper> passThroughMappers
    ) {
        if (mapper instanceof PassThroughObjectMapper passThroughObjectMapper) {
            passThroughMappers.add(passThroughObjectMapper);
            objectMappers.add(passThroughObjectMapper);
        } else if (mapper instanceof ObjectMapper objectMapper) {
            objectMappers.add(objectMapper);
        } else if (mapper instanceof FieldMapper fieldMapper) {
            fieldMappers.add(fieldMapper);
        } else if (mapper instanceof FieldAliasMapper fieldAliasMapper) {
            fieldAliasMappers.add(fieldAliasMapper);
        } else {
            throw new IllegalStateException("Unrecognized mapper type [" + mapper.getClass().getSimpleName() + "].");
        }

        for (Mapper child : mapper) {
            collect(child, objectMappers, fieldMappers, fieldAliasMappers, passThroughMappers);
        }
    }

    /**
     * Creates a new {@link MappingLookup} instance given the provided mappers and mapping.
     * Note that the provided mappings are not re-parsed but only exposed as-is. No consistency is enforced between
     * the provided mappings and set of mappers.
     * This is a commodity method to be used in tests, or whenever no mappings are defined for an index.
     * When creating a MappingLookup through this method, its exposed functionalities are limited as it does not
     * hold a valid {@link DocumentParser}, {@link IndexSettings} or {@link IndexAnalyzers}.
     *
     * @param mapping the mapping
     * @param mappers the field mappers
     * @param objectMappers the object mappers
     * @param aliasMappers the field alias mappers
     * @param passThroughMappers the pass-through mappers
     * @return the newly created lookup instance
     */
    public static MappingLookup fromMappers(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers,
        Collection<PassThroughObjectMapper> passThroughMappers
    ) {
        return new MappingLookup(mapping, mappers, objectMappers, aliasMappers, passThroughMappers);
    }

    public static MappingLookup fromMappers(Mapping mapping, Collection<FieldMapper> mappers, Collection<ObjectMapper> objectMappers) {
        return new MappingLookup(mapping, mappers, objectMappers, List.of(), List.of());
    }

    private MappingLookup(
        Mapping mapping,
        Collection<FieldMapper> mappers,
        Collection<ObjectMapper> objectMappers,
        Collection<FieldAliasMapper> aliasMappers,
        Collection<PassThroughObjectMapper> passThroughMappers
    ) {
        this.totalFieldsCount = mapping.getRoot().getTotalFieldsCount();
        this.mapping = mapping;
        Map<String, Mapper> fieldMappers = new HashMap<>();
        Map<String, ObjectMapper> objects = new HashMap<>();

        List<NestedObjectMapper> nestedMappers = new ArrayList<>();
        for (ObjectMapper mapper : objectMappers) {
            if (objects.put(mapper.fullPath(), mapper) != null) {
                throw new MapperParsingException("Object mapper [" + mapper.fullPath() + "] is defined more than once");
            }
            if (mapper.isNested()) {
                nestedMappers.add((NestedObjectMapper) mapper);
            }
        }
        this.nestedLookup = NestedLookup.build(nestedMappers);

        final Map<String, NamedAnalyzer> indexAnalyzersMap = new HashMap<>();
        final List<FieldMapper> indexTimeScriptMappers = new ArrayList<>();
        for (FieldMapper mapper : mappers) {
            if (objects.containsKey(mapper.fullPath())) {
                throw new MapperParsingException("Field [" + mapper.fullPath() + "] is defined both as an object and a field");
            }
            if (fieldMappers.put(mapper.fullPath(), mapper) != null) {
                throw new MapperParsingException("Field [" + mapper.fullPath() + "] is defined more than once");
            }
            indexAnalyzersMap.putAll(mapper.indexAnalyzers());
            if (mapper.hasScript()) {
                indexTimeScriptMappers.add(mapper);
            }
        }

        for (FieldAliasMapper aliasMapper : aliasMappers) {
            if (objects.containsKey(aliasMapper.fullPath())) {
                throw new MapperParsingException("Alias [" + aliasMapper.fullPath() + "] is defined both as an object and an alias");
            }
            if (fieldMappers.put(aliasMapper.fullPath(), aliasMapper) != null) {
                throw new MapperParsingException("Alias [" + aliasMapper.fullPath() + "] is defined both as an alias and a concrete field");
            }
        }

        PassThroughObjectMapper.checkForDuplicatePriorities(passThroughMappers);
        final Collection<RuntimeField> runtimeFields = mapping.getRoot().runtimeFields();
        this.fieldTypeLookup = new FieldTypeLookup(mappers, aliasMappers, passThroughMappers, runtimeFields);

        Map<String, InferenceFieldMetadata> inferenceFields = new HashMap<>();
        List<String> syntheticVectorFields = new ArrayList<>();
        for (FieldMapper mapper : mappers) {
            if (mapper instanceof InferenceFieldMapper inferenceFieldMapper) {
                inferenceFields.put(mapper.fullPath(), inferenceFieldMapper.getMetadata(fieldTypeLookup.sourcePaths(mapper.fullPath())));
            }
            if (mapper.syntheticVectorsLoader() != null) {
                syntheticVectorFields.add(mapper.fullPath());
            }
        }
        this.inferenceFields = Map.copyOf(inferenceFields);
        this.syntheticVectorFields = Set.copyOf(syntheticVectorFields);

        if (runtimeFields.isEmpty()) {
            // without runtime fields this is the same as the field type lookup
            this.indexTimeLookup = fieldTypeLookup;
        } else {
            this.indexTimeLookup = new FieldTypeLookup(mappers, aliasMappers, passThroughMappers, Collections.emptyList());
        }
        // make all fields into compact+fast immutable maps
        this.fieldMappers = Map.copyOf(fieldMappers);
        this.objectMappers = Map.copyOf(objects);
        this.runtimeFieldMappersCount = runtimeFields.size();
        this.indexAnalyzersMap = Map.copyOf(indexAnalyzersMap);
        this.indexTimeScriptMappers = List.copyOf(indexTimeScriptMappers);

        runtimeFields.stream().flatMap(RuntimeField::asMappedFieldTypes).map(MappedFieldType::name).forEach(this::validateDoesNotShadow);
        assert assertMapperNamesInterned(this.fieldMappers, this.objectMappers);
    }

    private static boolean assertMapperNamesInterned(Map<String, Mapper> mappers, Map<String, ObjectMapper> objectMappers) {
        mappers.forEach(MappingLookup::assertNamesInterned);
        objectMappers.forEach(MappingLookup::assertNamesInterned);
        return true;
    }

    private static void assertNamesInterned(String name, Mapper mapper) {
        assert name == name.intern();
        assert mapper.fullPath() == mapper.fullPath().intern();
        assert mapper.leafName() == mapper.leafName().intern();
        if (mapper instanceof ObjectMapper) {
            ((ObjectMapper) mapper).mappers.forEach(MappingLookup::assertNamesInterned);
        }
    }

    /**
     * Returns the leaf mapper associated with this field name. Note that the returned mapper
     * could be either a concrete {@link FieldMapper}, or a {@link FieldAliasMapper}.
     *
     * To access a field's type information, {@link MapperService#fieldType} should be used instead.
     */
    public Mapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    FieldTypeLookup fieldTypesLookup() {
        return fieldTypeLookup;
    }

    /**
     * Returns the total number of fields defined in the mappings, including field mappers, object mappers as well as runtime fields.
     */
    public long getTotalFieldsCount() {
        return totalFieldsCount;
    }

    /**
     * Returns the total number of mappers defined in the mappings, including field mappers and their sub-fields
     * (which are not explicitly defined in the mappings), multi-fields, object mappers, runtime fields and metadata field mappers.
     */
    public long getTotalMapperCount() {
        return fieldMappers.size() + objectMappers.size() + runtimeFieldMappersCount;
    }

    FieldTypeLookup indexTimeLookup() {
        return indexTimeLookup;
    }

    List<FieldMapper> indexTimeScriptMappers() {
        return indexTimeScriptMappers;
    }

    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unmappedFieldAnalyzer) {
        final NamedAnalyzer analyzer = indexAnalyzersMap.get(field);
        if (analyzer != null) {
            return analyzer;
        }
        return unmappedFieldAnalyzer.apply(field);
    }

    /**
     * Returns an iterable over all the registered field mappers (including alias mappers)
     */
    public Iterable<Mapper> fieldMappers() {
        return fieldMappers.values();
    }

    void checkLimits(IndexSettings settings) {
        checkFieldLimit(settings.getMappingTotalFieldsLimit());
        checkObjectDepthLimit(settings.getMappingDepthLimit());
        checkFieldNameLengthLimit(settings.getMappingFieldNameLengthLimit());
        checkNestedLimit(settings.getMappingNestedFieldsLimit());
        checkDimensionFieldLimit(settings.getMappingDimensionFieldsLimit());
    }

    private void checkFieldLimit(long limit) {
        checkFieldLimit(limit, 0);
    }

    void checkFieldLimit(long limit, int additionalFieldsToAdd) {
        if (exceedsLimit(limit, additionalFieldsToAdd)) {
            throw new IllegalArgumentException(
                "Limit of total fields ["
                    + limit
                    + "] has been exceeded"
                    + (additionalFieldsToAdd > 0 ? " while adding new fields [" + additionalFieldsToAdd + "]" : "")
            );
        }
    }

    boolean exceedsLimit(long limit, int additionalFieldsToAdd) {
        return remainingFieldsUntilLimit(limit) < additionalFieldsToAdd;
    }

    long remainingFieldsUntilLimit(long mappingTotalFieldsLimit) {
        return mappingTotalFieldsLimit - totalFieldsCount;
    }

    private void checkDimensionFieldLimit(long limit) {
        long dimensionFieldCount = fieldMappers.values()
            .stream()
            .filter(m -> m instanceof FieldMapper && ((FieldMapper) m).fieldType().isDimension())
            .count();
        if (dimensionFieldCount > limit) {
            throw new IllegalArgumentException("Limit of total dimension fields [" + limit + "] has been exceeded");
        }
    }

    private void checkObjectDepthLimit(long limit) {
        for (String objectPath : objectMappers.keySet()) {
            checkObjectDepthLimit(limit, objectPath);
        }
    }

    static void checkObjectDepthLimit(long limit, String objectPath) {
        int numDots = 0;
        for (int i = 0; i < objectPath.length(); ++i) {
            if (objectPath.charAt(i) == '.') {
                numDots += 1;
            }
        }
        final int depth = numDots + 2;
        if (depth > limit) {
            throw new IllegalArgumentException(
                "Limit of mapping depth [" + limit + "] has been exceeded due to object field [" + objectPath + "]"
            );
        }
    }

    private void checkFieldNameLengthLimit(long limit) {
        validateMapperNameIn(objectMappers.values(), limit);
        validateMapperNameIn(fieldMappers.values(), limit);
    }

    private static void validateMapperNameIn(Collection<? extends Mapper> mappers, long limit) {
        for (Mapper mapper : mappers) {
            String name = mapper.leafName();
            if (name.length() > limit) {
                throw new IllegalArgumentException("Field name [" + name + "] is longer than the limit of [" + limit + "] characters");
            }
        }
    }

    private void checkNestedLimit(long limit) {
        long actualNestedFields = 0;
        for (ObjectMapper objectMapper : objectMappers.values()) {
            if (objectMapper.isNested()) {
                actualNestedFields++;
            }
        }
        if (actualNestedFields > limit) {
            throw new IllegalArgumentException("Limit of nested fields [" + limit + "] has been exceeded");
        }
    }

    public Map<String, ObjectMapper> objectMappers() {
        return objectMappers;
    }

    /**
     * Returns a map containing all fields that require to run inference (through the {@link InferenceService} prior to indexation.
     */
    public Map<String, InferenceFieldMetadata> inferenceFields() {
        return inferenceFields;
    }

    public Set<String> syntheticVectorFields() {
        return syntheticVectorFields;
    }

    public NestedLookup nestedLookup() {
        return nestedLookup;
    }

    public boolean isMultiField(String field) {
        if (fieldMappers.containsKey(field) == false) {
            return false;
        }
        // Is it a runtime field?
        if (indexTimeLookup.get(field) != fieldTypeLookup.get(field)) {
            return false;
        }
        String sourceParent = parentObject(field);
        return sourceParent != null && fieldMappers.containsKey(sourceParent);
    }

    public boolean isObjectField(String field) {
        return objectMappers.containsKey(field);
    }

    private static String parentObject(String field) {
        int lastDot = field.lastIndexOf('.');
        if (lastDot == -1) {
            return null;
        }
        return field.substring(0, lastDot);
    }

    /**
     * Returns a set of field names that match a regex-like pattern
     *
     * All field names in the returned set are guaranteed to resolve to a field
     *
     * @param pattern the pattern to match field names against
     */
    public Set<String> getMatchingFieldNames(String pattern) {
        return fieldTypeLookup.getMatchingFieldNames(pattern);
    }

    /**
     * @return A map from field name to the MappedFieldType
     */
    public Map<String, MappedFieldType> getFullNameToFieldType() {
        return fieldTypeLookup.getFullNameToFieldType();
    }

    /**
     * Returns the mapped field type for the given field name.
     */
    public MappedFieldType getFieldType(String field) {
        return fieldTypesLookup().get(field);
    }

    /**
     * Given a concrete field name, return its paths in the _source.
     *
     * For most fields, the source path is the same as the field itself. However
     * there are cases where a field's values are found elsewhere in the _source:
     *   - For a multi-field, the source path is the parent field.
     *   - One field's content could have been copied to another through copy_to.
     *
     * @param field The field for which to look up the _source path. Note that the field
     *              should be a concrete field and *not* an alias.
     * @return A set of paths in the _source that contain the field's values.
     */
    public Set<String> sourcePaths(String field) {
        return fieldTypesLookup().sourcePaths(field);
    }

    /**
     * If field is a leaf multi-field return the path to the parent field. Otherwise, return null.
     */
    public String parentField(String field) {
        return fieldTypesLookup().parentField(field);
    }

    /**
     * Returns true if the index has mappings. An index does not have mappings only if it was created
     * without providing mappings explicitly, and no documents have yet been indexed in it.
     * @return true if the current index has mappings, false otherwise
     */
    public boolean hasMappings() {
        return this != EMPTY;
    }

    /**
     * Will there be {@code _source}.
     */
    public boolean isSourceEnabled() {
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        return sfm != null && sfm.enabled();
    }

    /**
     * Does the source need to be rebuilt on the fly?
     */
    public boolean isSourceSynthetic() {
        SourceFieldMapper sfm = mapping.getMetadataMapperByClass(SourceFieldMapper.class);
        return sfm != null && sfm.isSynthetic();
    }

    /**
     * Build something to load source {@code _source}.
     */
    public SourceLoader newSourceLoader(@Nullable SourceFilter filter, SourceFieldMetrics metrics) {
        if (isSourceSynthetic()) {
            return new SourceLoader.Synthetic(filter, () -> mapping.syntheticFieldLoader(filter), metrics);
        }
        var syntheticVectorsLoader = mapping.syntheticVectorsLoader(filter);
        if (syntheticVectorsLoader != null) {
            return new SourceLoader.SyntheticVectors(removeExcludedSyntheticVectorFields(filter), syntheticVectorsLoader);
        }
        return filter == null ? SourceLoader.FROM_STORED_SOURCE : new SourceLoader.Stored(filter);
    }

    private SourceFilter removeExcludedSyntheticVectorFields(@Nullable SourceFilter filter) {
        if (filter == null || filter.getExcludes().length == 0) {
            return filter;
        }
        List<String> newExcludes = new ArrayList<>();
        for (var exclude : filter.getExcludes()) {
            if (syntheticVectorFields().contains(exclude) == false) {
                newExcludes.add(exclude);
            }
        }
        if (newExcludes.isEmpty() && filter.getIncludes().length == 0) {
            return null;
        }
        return new SourceFilter(filter.getIncludes(), newExcludes.toArray(String[]::new));
    }

    /**
     * Returns if this mapping contains a data-stream's timestamp meta-field and this field is enabled.
     * Only indices that are a part of a data-stream have this meta-field enabled.
     * @return {@code true} if contains an enabled data-stream's timestamp meta-field, {@code false} otherwise.
     */
    public boolean isDataStreamTimestampFieldEnabled() {
        DataStreamTimestampFieldMapper dtfm = mapping.getMetadataMapperByClass(DataStreamTimestampFieldMapper.class);
        return dtfm != null && dtfm.isEnabled();
    }

    /**
     * Returns if this mapping contains a timestamp field that is of type date, has doc values, and is either indexed or uses a doc values
     * skipper.
     * @return {@code true} if contains a timestamp field of type date that has doc values and is either indexed or uses a doc values
     * skipper, {@code false} otherwise.
     */
    public boolean hasTimestampField() {
        final MappedFieldType mappedFieldType = fieldTypesLookup().get(DataStream.TIMESTAMP_FIELD_NAME);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType dateMappedFieldType) {
            return dateMappedFieldType.hasDocValues() && (dateMappedFieldType.isIndexed() || dateMappedFieldType.hasDocValuesSkipper());
        } else {
            return false;
        }
    }

    /**
     * Key for the lookup to be used in caches.
     */
    public CacheKey cacheKey() {
        return cacheKey;
    }

    /**
     * Returns the mapping source that this lookup originated from
     * @return the mapping source
     */
    public Mapping getMapping() {
        return mapping;
    }

    /**
     * Check if the provided {@link MappedFieldType} shadows a dimension
     * or metric field.
     */
    public void validateDoesNotShadow(String name) {
        MappedFieldType shadowed = indexTimeLookup.get(name);
        if (shadowed == null) {
            return;
        }
        if (shadowed.isDimension()) {
            throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_dimension");
        }
        if (shadowed.getMetricType() != null) {
            throw new MapperParsingException("Field [" + name + "] attempted to shadow a time_series_metric");
        }
    }
}
