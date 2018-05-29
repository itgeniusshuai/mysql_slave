package controllers

import "github.com/kataras/iris"

func Index(ctx iris.Context) {
	// Bind: {{.message}} with "Hello world!"
	ctx.ViewData("message", "Hello world!")
	// Render template file: ./views/hello.html
	ctx.View("index.html")
}
