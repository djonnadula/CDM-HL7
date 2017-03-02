val table = "hl7_msh_data"
table.substring(table.indexOf("_")+1, table.lastIndexOf("_"))
"hive/_HOST@HCA.CORPAD.NET".split("[/@]").foreach(println(_))

println("EPIC".startsWith("EPIC_MSLP_ADT_37357071"))