/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.step.repack

import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest
import org.opensearch.core.rest.RestStatus
import org.opensearch.indexmanagement.indexstatemanagement.action.RepackAction
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.AttemptUpdateIndexSettingsStep.Companion.OLD_CODEC_MODE_KEY
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.AttemptUpdateIndexSettingsStep.Companion.OLD_CODEC_MODE_VALUE
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import java.time.Duration
import java.time.Instant

class WaitForRepackCompletionStep(private val action: RepackAction) : Step(NAME) {
    private val logger = LogManager.getLogger(javaClass)
    private val info = mutableMapOf<String, Any>()
    private var stepStatus = StepStatus.STARTING

    @Suppress("TooGenericExceptionCaught", "ReturnCount")
    override suspend fun execute(): Step {
        val context = this.context ?: return this
        val indexName = context.metadata.index

        val oldSegments = segmentsWithPreviousMode(context, indexName) ?: return this

        if (oldSegments == 0) {
            val message = getWaitingMessage(indexName)
            logger.info(message)
            stepStatus = StepStatus.COMPLETED
            info["message"] = message
        } else {
            val timeWaitingForForceMerge: Duration = Duration.between(getActionStartTime(context), Instant.now())
            // Get ActionTimeout if given, otherwise use default timeout of 12 hours
            val timeoutInSeconds: Long = action.configTimeout?.timeout?.seconds ?: REPACK_TIMEOUT_IN_SECONDS

            if (timeWaitingForForceMerge.seconds > timeoutInSeconds) {
                logger.error(
                    "Repack on [$indexName] timed out with" +
                        " [$oldSegments] segments with older codec/mode",
                )

                stepStatus = StepStatus.FAILED
                info["message"] to getFailedTimedOutMessage(indexName)
            } else {
                logger.debug(
                    "Force merge still running on [$indexName] with" +
                        " [$oldSegments] segments with older codec/mode",
                )

                stepStatus = StepStatus.CONDITION_NOT_MET
                info["message"] to getWaitingMessage(indexName)
            }
        }

        return this
    }

    private suspend fun segmentsWithPreviousMode(context: StepContext, indexName: String): Int? {
        val req = IndicesSegmentsRequest(indexName)
        val resp = context.client.admin().indices().suspendUntil { segments(req, it) }
        if (resp.status != RestStatus.OK) {
            val message = getFailedSegmentCheckMessage(indexName)
            logger.error("$message - ${resp.status}")
            stepStatus = StepStatus.FAILED
            info["message"] = getFailedSegmentCheckMessage(indexName)
            return null
        }
        val indexSegments = resp.indices[indexName]
        if (indexSegments == null) {
            logger.warn("Cannot find segments for index: $indexName")
            return 0
        }
        val attributeKey = info[OLD_CODEC_MODE_KEY]
        val attributeValue = info[OLD_CODEC_MODE_VALUE]
        val segmentsWithOldMode = indexSegments.shards.values
            .flatMap { indexShard -> indexShard.shards.flatMap { it.segments } }
            .filter { segment ->
                segment.attributes.containsKey(attributeKey) && segment.attributes[attributeKey] == attributeValue
            }

        return segmentsWithOldMode.size
    }
    private fun getActionStartTime(context: StepContext): Instant {
        val managedIndexMetaData = context.metadata
        val startTime = managedIndexMetaData.actionMetaData?.startTime ?: return Instant.now()

        return Instant.ofEpochMilli(startTime)
    }

    override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData =
        currentMetadata.copy(
            stepMetaData = StepMetaData(
                AttemptUpdateIndexSettingsStep.NAME,
                getStepStartTime(currentMetadata).toEpochMilli(),
                stepStatus,
            ),
            transitionTo = null,
            info = info,
        )

    override fun isIdempotent(): Boolean = true

    companion object {
        const val NAME = "wait_for_repack_completion_step"
        const val REPACK_TIMEOUT_IN_SECONDS = 43200L // 12 hours

        fun getFailedTimedOutMessage(index: String) = "Repack timed out [index=$index]"
        fun getFailedSegmentCheckMessage(index: String) =
            "Failed to check segments when waiting for repack to complete [index=$index]"

        fun getWaitingMessage(index: String) = "Waiting for repack to complete [index=$index]"
        fun getSuccessMessage(index: String) = "Successfully repack all segments [index=$index]"
    }
}
