package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
)

var compiledRegex = regexp.MustCompile(`([0-9]|[1-9][0-9]|[1-5][0-9][0-9]) (U\.[ ]?S\.)[ ]?(,)?[ ]?(at)?[ ]?([0-9]+)(, ([0-9]+))?( \(([0-9]+)\))?`)

func main() {
	initProcessing(os.Args[1])
}

func initProcessing(basePath string) {
	fmt.Println("Initiating...")
	startTime := time.Now()

	// get all files
	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(fmt.Sprintf("Found %d files", len(files)))
	fmt.Println("Processing...")

	listFileChannel := make(chan fs.FileInfo, len(files))
	fileContentChannel := make(chan map[string]string, len(files))
	fileResultChannel := make(chan []string, len(files))

	// initiate workers for file processing
	for i := 0; i < 100; i++ {
		go loadFileContent(listFileChannel, fileContentChannel, basePath)
		go processFileContent(fileContentChannel, fileResultChannel)
	}

	for i := 0; i < len(files); i++ {
		listFileChannel <- files[i]
	}
	close(listFileChannel)

	// append results to file
	f, err := os.Create("./result.csv")
	for i := 0; i < len(files); i++ {
		text := strings.Join(<-fileResultChannel, "")
		if _, err := f.WriteString(text); err != nil {
			log.Println(err)
		}
	}
	close(fileResultChannel)
	close(fileContentChannel)

	fmt.Println("Created result.csv")
	fmt.Println(fmt.Sprintf("Total processing time: %s", time.Since(startTime)))
}

func processFileContent(fileContentChannel chan map[string]string, fileResultChannel chan []string) {
	for fileContent := range fileContentChannel {
		var fileName string
		var content string

		for k, v := range fileContent {
			fileName = k
			content = v
		}

		annotationMap := map[string]int{}

		scanner := bufio.NewScanner(strings.NewReader(content))
		for scanner.Scan() {
			matches := compiledRegex.FindAllString(scanner.Text(), -1)

			for _, v := range matches {
				v = strings.ReplaceAll(v, "U. S.", "U.S.")
				parts := strings.Split(v, ",")
				v = parts[0]

				var pageInfo string
				if len(parts) > 1 {
					pageInfo = parts[1]
				}

				v = strings.ReplaceAll(v, "at", "")

				parts = strings.Split(v, " ")

				var partsReal []string
				for _, part := range parts {
					if len(part) != 0 {
						partsReal = append(partsReal, part)
					}
				}

				if len(partsReal) > 3 {
					partsReal = partsReal[:2]
				} else {
					key := strings.Join(partsReal, " ")
					keyFound := false

					for k := range annotationMap {
						if strings.HasPrefix(k, key) {
							partsReal = strings.Split(k, " ")
							keyFound = true
						}
					}

					if !keyFound && len(pageInfo) != 0 {
						partsReal = strings.Split(key, " ")
					}
				}
				v = strings.Join(partsReal, " ")

				if val, ok := annotationMap[v]; ok {
					annotationMap[v] = val + 1
				} else {
					annotationMap[v] = 1
				}
			}
		}
		err := scanner.Err()
		if err != nil {
			fmt.Println("Error reading file content", err)
		}

		var fileAnnotations []string
		for k, v := range annotationMap {
			fileAnnotations = append(fileAnnotations, fmt.Sprintf("%s,%s,%d\n", fileName, k, v))
		}

		fileResultChannel <- fileAnnotations
	}
}

func loadFileContent(listFileChannel <-chan fs.FileInfo, fileContentChannel chan map[string]string, basePath string) {
	for fileInfo := range listFileChannel {
		content, err := os.ReadFile(fmt.Sprintf("%s%c%s", basePath, os.PathSeparator, fileInfo.Name()))
		if err != nil {
			fmt.Println("Error reading file", err)
		}

		fileContentChannel <- map[string]string{
			fileInfo.Name(): string(content),
		}
	}
}
