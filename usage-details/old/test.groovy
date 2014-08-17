//def process=new ProcessBuilder("bash -c ls").redirectErrorStream(true).directory(new File("/tmp"))start()
//process.inputStream.eachLine {println it}

def command = """hadoop fs -lsr hdfs://localhost:9000/data/alpha | awk '{print $8}' | grep 'data\-.*\.dat' | grep ".*\/[0-9]\{14\}/""""		// Create the String
def proc = command.execute()                 // Call *execute* on the string
proc.waitFor()                               // Wait for the command to finish
println "stdout: ${proc.in.text}" 

def command2 = """echo yy"""// Create the String
def proc2 = command2.execute()                 // Call *execute* on the string
proc2.waitFor()                               // Wait for the command to finish
// Obtain status and output
//println "return code: ${ proc.exitValue()}"
//println "stderr: ${proc.err.text}"
println "stdout: ${proc2.in.text}" 
