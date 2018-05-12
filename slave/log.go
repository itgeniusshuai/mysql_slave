package slave

import "fmt"

func Print(format string,strs ...interface{}){
	fmt.Print(fmt.Sprintf(format,strs))
}

func Println(format string,strs ...interface{}){
	fmt.Println(fmt.Sprintf(format,strs))
}
