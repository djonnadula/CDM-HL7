package com.hca.cdm.kafka.util

import com.hca.cdm.log.Logg
import kafka.message.{Message, MessageAndMetadata}
import kafka.serializer.Decoder

/**
  * Created by Devaraj Jonnadula on 9/21/2016.
  */
class MessageHandler[K, V](override val topic: String, override val partition: Int,
                                private val rawMessage: Message, override val offset: Long,
                                override val keyDecoder: Decoder[K], override val valueDecoder: Decoder[V]) extends MessageAndMetadata(topic, partition, rawMessage, offset, keyDecoder, valueDecoder)
  with Logg {


    info("")


}
