package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.5840/mr"
import "unicode"
import "strings"
import "strconv"

// Map The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
// 对于每个输入文件，调用Map函数一次。第一个
// 参数是输入文件的名称，第二个参数是
// 文件的完整内容。您应该忽略输入文件名，
// 只看内容论证。返回值是一个键/值对的切片。
func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators. 如果一个字符不是字母，则被视为单词的分隔符。
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words. 将文件内容 contents 分割成一个单词数组 words，其中使用函数变量 ff 进行分隔。
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
