package main

import "github.com/joho/godotenv"

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic("unable to load .env")
	}
}

func main() {}
