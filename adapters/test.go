package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

func main() {
	pattern := regexp.MustCompile("(^.+Exception: .+)|(^\\s+at .+)|(^\\s+... \\d+ more)|(^\\s*Caused by:.+)")

	fmt.Println(pattern.MatchString("Exception in thread \"main\" java.lang.RuntimeException: Coucou Kibana"))
	fmt.Println(pattern.MatchString("        at ploup.Main.main(Main.java:6)"))
	fmt.Println(pattern.MatchString("Caused by: java.lang.NullPointerException: Bloup"))
	fmt.Println(pattern.MatchString("        ... 1 more"))
	fmt.Println(pattern.MatchString("Coucou"))

	c := make(chan string, 1)
	go func() {
		receive := true
		data := "Receive :"
		for receive == true {
			select {
			case res := <-c:
				fmt.Println("Receive data ", res)
				data = strings.Join([]string{data, res}, "\n")
			case <-time.After(time.Second * 2):
				receive = false
			}
		}
		fmt.Println(data)
	}()

	c <- "result 1"
	c <- "result 2"
	c <- "result 3"
	time.Sleep(time.Second * 1)
	c <- "result 4"
	c <- "result 5"
	time.Sleep(time.Second * 3)
}
