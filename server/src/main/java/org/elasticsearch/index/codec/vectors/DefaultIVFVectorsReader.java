/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.index.codec.vectors.BQSpaceUtils.transposeHalfByte;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
import static org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer.DEFAULT_LAMBDA;
import static org.elasticsearch.simdvec.ES91OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class DefaultIVFVectorsReader extends IVFVectorsReader implements OffHeapStats {
    private static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    public DefaultIVFVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        super(state, rawVectorsReader);
    }

    @Override
    CentroidIterator getCentroidIterator(FieldInfo fieldInfo, int numCentroids, IndexInput centroids, float[] targetQuery)
        throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        final float globalCentroidDp = fieldEntry.globalCentroidDp();
        final OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final int[] scratch = new int[targetQuery.length];
        final OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            ArrayUtil.copyArray(targetQuery),
            scratch,
            (byte) 4,
            fieldEntry.globalCentroid()
        );
        final byte[] quantized = new byte[targetQuery.length];
        for (int i = 0; i < quantized.length; i++) {
            quantized[i] = (byte) scratch[i];
        }
        final ES91Int4VectorsScorer scorer = ESVectorUtil.getES91Int4VectorsScorer(centroids, fieldInfo.getVectorDimension());
        NeighborQueue queue = new NeighborQueue(fieldEntry.numCentroids(), true);
        centroids.seek(0L);
        final float[] centroidCorrectiveValues = new float[3];
        for (int i = 0; i < numCentroids; i++) {
            final float qcDist = scorer.int4DotProduct(quantized);
            centroids.readFloats(centroidCorrectiveValues, 0, 3);
            final int quantizedCentroidComponentSum = Short.toUnsignedInt(centroids.readShort());
            float score = int4QuantizedScore(
                qcDist,
                queryParams,
                fieldInfo.getVectorDimension(),
                centroidCorrectiveValues,
                quantizedCentroidComponentSum,
                globalCentroidDp,
                fieldInfo.getVectorSimilarityFunction()
            );
            queue.add(i, score);
        }
        final long offset = centroids.getFilePointer();
        return new CentroidIterator() {
            @Override
            public boolean hasNext() {
                return queue.size() > 0;
            }

            @Override
            public long nextPostingListOffset() throws IOException {
                int centroidOrdinal = queue.pop();
                centroids.seek(offset + (long) Long.BYTES * centroidOrdinal);
                return centroids.readLong();
            }
        };
    }

    // TODO can we do this in off-heap blocks?
    private float int4QuantizedScore(
        float qcDist,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        int dims,
        float[] targetCorrections,
        int targetComponentSum,
        float centroidDp,
        VectorSimilarityFunction similarityFunction
    ) {
        float ax = targetCorrections[0];
        float lx = (targetCorrections[1] - ax) * FOUR_BIT_SCALE;
        float ay = queryCorrections.lowerInterval();
        float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
        float y1 = queryCorrections.quantizedComponentSum();
        float score = ax * ay * dims + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
        if (similarityFunction == EUCLIDEAN) {
            score = queryCorrections.additionalCorrection() + targetCorrections[2] - 2 * score;
            return Math.max(1 / (1f + score), 0);
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryCorrections.additionalCorrection() + targetCorrections[2] - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return Math.max((1f + score) / 2f, 0);
        }
    }

    @Override
    PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput indexInput, float[] target, IntPredicate needsScoring)
        throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        return new MemorySegmentPostingsVisitor(target, indexInput.clone(), entry, fieldInfo, needsScoring);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return Map.of();
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final float[] target;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final IntPredicate needsScoring;
        private final ES91OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];

        int[] docIdsScratch = new int[0];
        int vectors;
        boolean quantized = false;
        float centroidDp;
        final float[] centroid;
        long slicePos;
        OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        DocIdsWriter docIdsWriter = new DocIdsWriter();

        final float[] scratch;
        final int[] quantizationScratch;
        final byte[] quantizedQueryScratch;
        final OptimizedScalarQuantizer quantizer;
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            float[] target,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            IntPredicate needsScoring
        ) throws IOException {
            this.target = target;
            this.indexInput = indexInput;
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.needsScoring = needsScoring;
            centroid = new float[fieldInfo.getVectorDimension()];
            scratch = new float[target.length];
            quantizationScratch = new int[target.length];
            final int discretizedDimensions = discretize(fieldInfo.getVectorDimension(), 64);
            quantizedQueryScratch = new byte[QUERY_BITS * discretizedDimensions / 8];
            quantizedByteLength = discretizedDimensions / 8 + (Float.BYTES * 3) + Short.BYTES;
            quantizedVectorByteSize = (discretizedDimensions / 8);
            quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction(), DEFAULT_LAMBDA, 1);
            osqVectorsScorer = ESVectorUtil.getES91OSQVectorsScorer(indexInput, fieldInfo.getVectorDimension());
        }

        @Override
        public int resetPostingsScorer(long offset) throws IOException {
            quantized = false;
            indexInput.seek(offset);
            indexInput.readFloats(centroid, 0, centroid.length);
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            vectors = indexInput.readVInt();
            // read the doc ids
            docIdsScratch = vectors > docIdsScratch.length ? new int[vectors] : docIdsScratch;
            docIdsWriter.readInts(indexInput, vectors, docIdsScratch);
            slicePos = indexInput.getFilePointer();
            return vectors;
        }

        void scoreIndividually(int offset) throws IOException {
            // score individually, first the quantized byte chunk
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j + offset];
                if (doc != -1) {
                    indexInput.seek(slicePos + (offset * quantizedByteLength) + (j * quantizedVectorByteSize));
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    scores[j] = qcDist;
                }
            }
            // read in all corrections
            indexInput.seek(slicePos + (offset * quantizedByteLength) + (BULK_SIZE * quantizedVectorByteSize));
            indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
            indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
            for (int j = 0; j < BULK_SIZE; j++) {
                correctionsSum[j] = Short.toUnsignedInt(indexInput.readShort());
            }
            indexInput.readFloats(correctionsAdd, 0, BULK_SIZE);
            // Now apply corrections
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[offset + j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctionsLower[j],
                        correctionsUpper[j],
                        correctionsSum[j],
                        correctionsAdd[j],
                        scores[j]
                    );
                }
            }
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            // block processing
            int scoredDocs = 0;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            for (; i < limit; i += BULK_SIZE) {
                int docsToScore = BULK_SIZE;
                for (int j = 0; j < BULK_SIZE; j++) {
                    int doc = docIdsScratch[i + j];
                    if (needsScoring.test(doc) == false) {
                        docIdsScratch[i + j] = -1;
                        docsToScore--;
                    }
                }
                if (docsToScore == 0) {
                    continue;
                }
                quantizeQueryIfNecessary();
                indexInput.seek(slicePos + i * quantizedByteLength);
                if (docsToScore < BULK_SIZE / 2) {
                    scoreIndividually(i);
                } else {
                    osqVectorsScorer.scoreBulk(
                        quantizedQueryScratch,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        scores
                    );
                }
                for (int j = 0; j < BULK_SIZE; j++) {
                    int doc = docIdsScratch[i + j];
                    if (doc != -1) {
                        scoredDocs++;
                        knnCollector.collect(doc, scores[j]);
                    }
                }
            }
            // process tail
            for (; i < vectors; i++) {
                int doc = docIdsScratch[i];
                if (needsScoring.test(doc)) {
                    quantizeQueryIfNecessary();
                    indexInput.seek(slicePos + i * quantizedByteLength);
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    indexInput.readFloats(correctiveValues, 0, 3);
                    final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
                    float score = osqVectorsScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctiveValues[0],
                        correctiveValues[1],
                        quantizedComponentSum,
                        correctiveValues[2],
                        qcDist
                    );
                    scoredDocs++;
                    knnCollector.collect(doc, score);
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }

        private void quantizeQueryIfNecessary() {
            if (quantized == false) {
                System.arraycopy(target, 0, scratch, 0, target.length);
                if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                    VectorUtil.l2normalize(scratch);
                }
                queryCorrections = quantizer.scalarQuantize(scratch, quantizationScratch, (byte) 4, centroid);
                transposeHalfByte(quantizationScratch, quantizedQueryScratch);
                quantized = true;
            }
        }
    }

}
