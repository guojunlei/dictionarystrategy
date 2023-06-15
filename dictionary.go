package main

import (
	. "dictionary_strategy/functions"
	. "dictionary_strategy/structs"
	"fmt"
	"github.com/spf13/viper"
	"strconv"
	"strings"
	"sync"
	"time"
)

var reserveColumns = []string{
	"trade_date",
	"is_trade",
	"next_is_trade",
	"next_open_up",
	"next_return",
}

var selectNum int
var StartTime string
var factorDf *GoFrame
var mode int
var maxCore int
var maxNum int
var stockPath string
var factorPath string
var outPutPath string

func init() {
	config := viper.New()
	config.AddConfigPath(".")
	config.SetConfigName("config")
	config.SetConfigType("yaml")
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}
	selectNum = config.Get("para.selectnum").(int)
	StartTime = config.Get("para.starttime").(string)
	mode = config.Get("para.mode").(int)
	maxCore = config.Get("para.maxcore").(int)
	maxNum = config.Get("para.maxnum").(int)
	stockPath = config.Get("para.stockpath").(string)
	factorPath = config.Get("para.factorPath").(string)

	csvData, err := ReadCsvFile(factorPath + "factor_" + strconv.FormatInt(int64(mode), 10))
	ErrorExit(err)
	factorDf = CsvToFrame(csvData)

}

func main() {
	//导入数据
	csvData, err := ReadCsvFile(stockPath + "all_stock_data")
	ErrorExit(err)
	df := CsvToFrame(csvData)

	if mode == 1 {
		fmt.Println("合计组合数:", len(factorDf.Index))
		now := time.Now()
		var netAll []Net
		chanData := make(chan int, maxCore)
		chanNet := make(chan Net, len(factorDf.Index))
		for j := 0; j < len(factorDf.Index); j++ {
			go func(i int) {
				chanData <- 1
				factor := factorDf.Data[0].Data[i].(string)[2 : len(factorDf.Data[0].Data[i].(string))-2]
				direction := factorDf.Data[1].Data[i].(string)[2 : len(factorDf.Data[1].Data[i].(string))-2]
				factor = strings.ReplaceAll(factor, "'", "")
				factor = strings.ReplaceAll(factor, " ", "")
				direction = strings.ReplaceAll(direction, "'", "")
				direction = strings.ReplaceAll(direction, " ", "")
				factorName := strings.Split(factor, ",")
				factorDir := strings.Split(direction, ",")
				factorMap := make(map[string]bool, len(factorName))
				for i := 0; i < len(factorName); i++ {
					d, _ := strconv.ParseBool(factorDir[i])
					factorMap[factorName[i]] = d
				}
				newDf := DeleteUseless(df, factorName, reserveColumns)

				net := CalculateCurve(newDf, factorMap, selectNum, StartTime)
				chanNet <- net
			}(j)
			//netAll = append(netAll, net)
		}
		for i := 0; i < len(factorDf.Index); i++ {
			<-chanData
			netAll = append(netAll, <-chanNet)
		}
		//fmt.Println(netAll)
		fmt.Println("共用时:", time.Since(now))
		NetToCsv(&netAll, mode, outPutPath)
	}

	if mode != 1 {
		fmt.Println("合计组合数:", len(factorDf.Index))
		var mutex sync.RWMutex
		now := time.Now()
		netAll := make([]Net, 0, maxNum)
		chanData := make(chan int, maxCore)
		chanNet := make(chan Net, len(factorDf.Index))
		go func() {
			for j := 0; j < len(factorDf.Index); j++ {
				go func(i int) {
					chanData <- 1
					factor := factorDf.Data[0].Data[i].(string)[2 : len(factorDf.Data[0].Data[i].(string))-2]
					direction := factorDf.Data[1].Data[i].(string)[2 : len(factorDf.Data[1].Data[i].(string))-2]
					factor = strings.ReplaceAll(factor, "'", "")
					factor = strings.ReplaceAll(factor, " ", "")
					direction = strings.ReplaceAll(direction, "'", "")
					direction = strings.ReplaceAll(direction, " ", "")
					factorName := strings.Split(factor, ",")
					factorDir := strings.Split(direction, ",")
					factorMap := make(map[string]bool, len(factorName))

					for i := 0; i < len(factorName); i++ {
						d, _ := strconv.ParseBool(factorDir[i])
						factorMap[factorName[i]] = d
					}

					newDf := DeleteUseless(df, factorName, reserveColumns)
					net := CalculateCurve(newDf, factorMap, selectNum, StartTime)
					chanNet <- net
				}(j)
			}
		}()
		for i := 0; i < len(factorDf.Index); i++ {
			<-chanData
			n := <-chanNet
			if len(netAll) < maxNum {
				netAll = append(netAll, n)
			} else {
				mutex.RLock()
				DeleteMin(netAll, n)
				mutex.RUnlock()
			}
		}
		fmt.Println(netAll)
		fmt.Println("共用时:", time.Since(now))
		NetToCsv(&netAll, mode, outPutPath)
	}
}
