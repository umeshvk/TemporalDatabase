def process=new ProcessBuilder("./getInputFiles.sh")
    		.redirectErrorStream(true)
		.start()
process.inputStream.eachLine {println it}

