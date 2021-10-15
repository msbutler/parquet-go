package main

import (
	"bytes"
	"fmt"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"log"
	"os"

	//"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	var err error
	md := []string{
		"name=Name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY",
		"name=Age, type=INT32",
		"name=Id, type=INT64",
	}

	//write
	/*fw, err := local.NewLocalFileWriter("csv.parquet")
	if err != nil {
		log.Println("Can't open file", err)
		return
	}*/
	buf := bytes.NewBuffer([]byte{})
	pf := writerfile.NewWriterFile(buf)

	pw, err := writer.NewCSVWriter(md, pf, 4)
	if err != nil {
		log.Println("Can't create csv writer", err)
		return
	}

	num := 10
	for i := 0; i < num; i++ {

		data2 := []interface{}{
			"Student Name",
			int32(20 + i%5),
			int64(i),
		}
		if err = pw.Write(data2); err != nil {
			log.Println("Write error", err)
		}

	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}

	//write to file
	f, err := os.Create("csv_ex.parquet")
	if err != nil{
		log.Println("couldn't create file")
	}
	defer f.Close()

	n, err := f.Write(buf.Bytes())
	fmt.Printf("wrote %d bytes\n", n)


	// COULD CLOSE pf here, too!
	pf.Close()
}
