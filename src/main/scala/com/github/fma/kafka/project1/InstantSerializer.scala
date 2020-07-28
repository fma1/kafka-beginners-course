package com.github.fma.kafka.project1

import java.time.Instant

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

case object InstantSerializer
  extends CustomSerializer[Instant](
    _ =>
      ({
        case JString(s) => Instant.parse(s)
        case _ => null
      }, Map())
  )
