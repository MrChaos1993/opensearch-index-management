/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.common.settings.Settings
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.index.engine.EngineConfig
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.AttemptCreateRepackJobStep
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.AttemptUpdateIndexSettingsStep
import org.opensearch.indexmanagement.indexstatemanagement.step.repack.WaitForRepackCompletionStep
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.Step
import org.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RepackAction(
    val newCodec: String,
    actionIndex: Int,
) : Action(NAME, actionIndex) {
    init {
        // Just to validate codec name: parse method will be called and raise an Exception if codec missing
        EngineConfig.INDEX_CODEC_SETTING.get(Settings.builder().put(EngineConfig.INDEX_CODEC_SETTING.key, newCodec).build())
    }

    private val attemptUpdateIndexSettingsStep = AttemptUpdateIndexSettingsStep(this)
    private val attemptCreateRepackJobStep = AttemptCreateRepackJobStep()
    private val waitForRepackCompletionStep = WaitForRepackCompletionStep(this)

    // Using a LinkedHashMap here to maintain order of steps for getSteps() while providing a convenient way to
    // get the current Step object using the current step's name in getStepToExecute()
    private val stepNameToStep: LinkedHashMap<String, Step> =
        linkedMapOf(
            AttemptUpdateIndexSettingsStep.NAME to attemptUpdateIndexSettingsStep,
            AttemptCreateRepackJobStep.NAME to attemptCreateRepackJobStep,
            WaitForRepackCompletionStep.NAME to waitForRepackCompletionStep,
        )

    override fun getSteps(): List<Step> = stepNameToStep.values.toList()

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        val managedIndexMetaData = context.metadata
        // If stepMetaData is null, return the first step in RepackAction
        val stepMetaData = managedIndexMetaData.stepMetaData ?: return attemptUpdateIndexSettingsStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action (assumed to be from the previous action in the policy), return
        // the first step in RepackAction
        if (!stepNameToStep.containsKey(currentStep)) return attemptUpdateIndexSettingsStep

        val currentStepStatus = stepMetaData.stepStatus

        // If the current step has completed, return the next step
        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            return when (currentStep) {
                AttemptUpdateIndexSettingsStep.NAME -> attemptCreateRepackJobStep
                AttemptCreateRepackJobStep.NAME -> waitForRepackCompletionStep
                // Shouldn't hit this case but including it so that the "when" expression is exhaustive
                else -> stepNameToStep[currentStep]!!
            }
        }

        // If the current step has not completed, return it
        return stepNameToStep[currentStep]!!
    }

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(NEW_CODEC_FIELD, newCodec)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(newCodec)
        out.writeInt(actionIndex)
    }

    companion object {
        const val NAME = "repack"
        const val NEW_CODEC_FIELD = "new_codec"
    }
}
