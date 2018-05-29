package controllers

import (
	"github.com/kataras/iris"
	"github.com/kataras/iris/mvc"
	"github.com/kataras/iris/middleware/logger"
)

type IndexController struct {

}

func InitWeb(){
	app := iris.New()
	// Optionally, add two built'n handlers
	// that can recover from any http-relative panics
	// and log the requests to the terminal.
	app.Use(logger.New(),before)
	app.Done(after)
	app.RegisterView(iris.HTML("./static/views",".html"))
	app.StaticWeb("/static/js","./static/js")

	mvc.New(app).Handle(new(IndexController))

	app.Run(iris.Addr(":8080"))
}

func before(ctx iris.Context){
	ctx.Application().Logger().Println("before request")
	ctx.Next()
}

func after(ctx iris.Context){
	ctx.Application().Logger().Println("after request")
	ctx.Next()
}



func (this *IndexController)BeforeActivation(b mvc.BeforeActivation){
	anyMiddlewareHere := func(ctx iris.Context) {
		ctx.Application().Logger().Warnf("Inside /custom_path")
		ctx.Next()
	}
	b.Handle("GET", "/index", "Index", anyMiddlewareHere)

}

func (c *IndexController) Get() mvc.Response {
	return mvc.Response{
		ContentType: "text/html",
		Text:        "<h1>Welcome</h1>",
	}
}

func (this *IndexController)GetHello()string{
	return "hello"
}

func (this *IndexController)Index(ctx iris.Context) {
	// Bind: {{.message}} with "Hello world!"
	ctx.ViewData("message", "Hello world!")
	// Render template file: ./views/hello.html
	ctx.View("index.html")

}
