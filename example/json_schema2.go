package main

import (
	"bytes"
	"fmt"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"log"
	"os"

	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	var err error
	var md = `
    {
        "Tag": "name=parquet-go-root, repetitiontype=REQUIRED",
        "Fields": [
			{"Tag": "name=name, inname=Name, type=BYTE_ARRAY, convertedtype=UTF8,repetitiontype=REQUIRED"},
			{"Tag": "name=age, inname=Age, type=INT32, repetitiontype=REQUIRED"},
			{"Tag": "name=id, inname=ID, type=INT64, repetitiontype=REQUIRED"}
		]
	}
`
	//write
	buf := bytes.NewBuffer([]byte{})
	pf := writerfile.NewWriterFile(buf)
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriter(pf,md,4)
	if err != nil {
		log.Println("Can't create json writer", err)
		return
	}

	num := 10
	for i := 0; i < num; i++ {
		rec := []interface{}{
			"Student Name",
			int32(20+i%5),
			int64(i)}
		if err = pw.Write(rec); err != nil {
			log.Println("Write error", err)
		}

	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}

	//write to file
	f, err := os.Create("json_schema_ex.parquet")
	if err != nil{
		log.Println("couldn't create file")
	}
	defer f.Close()

	n, err := f.Write(buf.Bytes())
	fmt.Printf("wrote %d bytes\n", n)
	pf.Close()
}
