# 使用Hugo+FixIt主题搭建个人博客


<!--more-->
记录一下使用Hugo+FixIt主题搭建个人博客的过程。
## 准备
安装Hugo
```go
go install github.com/gohugoio/hugo@latest
```
## 安装
### 创建项目
使用Hugo创建新网站
```Bash
hugo new site website
```

安装主题，我这里选择了[FixIt](https://github.com/hugo-fixit/FixIt)主题。
```Bash
git clone https://github.com/hugo-fixit/FixIt.git themes/FixIt
```
### 配置
通过配置文件config.toml对网站进行配置。`themes/FixIt/config.toml`是FixIt主题的配置模版文件，每个配置项有详细的注释说明，可参考进行配置。

### 创建文章
通过Hugo命令创建文章：
```Bash
hugo new posts/first_post.md
```

### 本地启动
```Bash
hugo server
```

## 部署发布

---

> 作者: [raywang](https://github.com/alexwanglei)  
> URL: https://alexwanglei.github.io/posts/create_hugo_blog/  

