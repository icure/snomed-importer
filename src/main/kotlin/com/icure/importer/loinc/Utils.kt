package com.icure.importer.loinc

fun getLoincFQN(fields: List<String>, columns: Map<String, Int>) =
    "${fields[columns["COMPONENT"]!!]}:" +
            "${fields[columns["PROPERTY"]!!]}:" +
            "${fields[columns["TIME_ASPCT"]!!]}:" +
            "${fields[columns["SYSTEM"]!!]}:" +
            fields[columns["SCALE_TYP"]!!] +
            ((":" + fields[columns["METHOD_TYP"]!!]).takeIf { it != ":" } ?: "")