/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.action

import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken
import org.opensearch.indexmanagement.indexstatemanagement.action.RepackAction.Companion.NEW_CODEC_FIELD
import org.opensearch.indexmanagement.spi.indexstatemanagement.Action
import org.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser

class RepackActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val newCodec = sin.readString()
        val actionIndex = sin.readInt()
        return RepackAction(newCodec, actionIndex)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var newCodecName: String? = null

        ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                NEW_CODEC_FIELD -> newCodecName = xcp.text()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in RepackActionConfig.")
            }
        }

        return RepackAction(
            requireNotNull(newCodecName) { "RepackActionConfig newCodecName is null" },
            index,
        )
    }

    override fun getActionType(): String = RepackAction.NAME
}
