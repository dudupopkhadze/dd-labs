package mr

import (
	"io/ioutil"
	"log"
	"os"
)

func renameFile(oldName string, newName string) {
	err := os.Rename(oldName, newName)
	if err != nil {
		log.Fatalln(err)
	}
}

func openFile(file string) (openedFile *os.File) {
	openedFile, errr := os.Open(file)
	if errr != nil {
		log.Fatal("Can not open File:", file)
	}
	return
}

func openFileWithWrite(name string) (file *os.File) {
	file, err := os.OpenFile(name,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("Error opening file:", name)
	}
	return
}

func createFile(name string) (file *os.File) {
	file, errr := os.Create(name)
	if errr != nil {
		log.Fatal("Can not create file with name :", name)
	}
	return
}

func readFileAsString(file *os.File) string {
	r, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalln("error reading file for map ")
	}
	return string(r)
}
