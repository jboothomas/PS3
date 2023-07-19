package cmd

import "fmt"

// util to output Debug information to stdout
func VerbosePrintln(str ...interface{}) {
	if fVerbose || fDebug || fTrace {
		fmt.Println(str...)
		return
	}
}

// util to output Debug information to stdout
func DebugPrintln(str ...interface{}) {
	if fDebug || fTrace {
		fmt.Println(str...)
		return
	}
}

// util to output Trace information to stdout
func TracePrintln(str ...interface{}) {
	if fTrace {
		fmt.Println(str...)
		return
	}
}
