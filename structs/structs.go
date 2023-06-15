package structs

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Series struct {
	Index []int
	Data  []interface{}
}

type GoFrame struct {
	Columns []string
	Index   []int
	Data    []Series
}

type Res struct {
	T   string
	Ret float64
}

type Net struct {
	F []string
	D []string
	N float64
}

// Series方法

func (sr *Series) AppendRow(s string) {
	num := len(sr.Index)
	if num == 0 {
		sr.Index = append(sr.Index, 0)
	} else {
		sr.Index = append(sr.Index, num)
	}
	sr.Data = append(sr.Data, s)
}

func (sr *Series) Rank(b bool) []interface{} {
	var d []float64
	for i := 0; i < len(sr.Data); i++ {
		e, _ := strconv.ParseFloat(sr.Data[i].(string), 64)
		d = append(d, e)
	}
	f := make([]float64, len(d))
	copy(f, d)
	sort.Float64s(f)
	if !b {
		Reverse(f)
	}
	rankIndex := make([]int, len(d))
	for i := 0; i < len(d); i++ {
		for j := 0; j < len(f); j++ {
			if d[i] == f[j] {
				rankIndex[i] = j + 1
			}
		}
	}
	rIndex := make([]interface{}, len(rankIndex))
	for i := 0; i < len(rIndex); i++ {
		rIndex[i] = rankIndex[i]
	}
	return rIndex
}

//GoFrame方法

func (gf *GoFrame) Find(n string) (Series, error) {
	var ind int
	for i := 0; i < len(gf.Columns); i++ {
		if n == gf.Columns[i] {
			ind = i
			break
		}
	}
	newS := gf.Data[ind]
	return newS, nil
}

func (gf *GoFrame) DropNull(n string) {
	nSlice, err := gf.Find(n)
	ErrorExit(err)

	var newIndex []int
	for i, v := range nSlice.Data {
		if v != "" {
			newIndex = append(newIndex, i)
		}
	}

	nIndex := make([]int, len(newIndex))
	newData := make([]Series, len(gf.Data))

	for j, vs := range gf.Data {
		nData := make([]interface{}, len(newIndex))
		for i, v := range newIndex {
			nData[i] = vs.Data[v]
			nIndex[i] = v
		}
		nS := Series{Index: nIndex, Data: nData}
		newData[j] = nS
	}
	gf.Data = newData
	gf.Index = gf.Data[0].Index
}

func (gf *GoFrame) SelectTime(n string, s, e string) {
	nSlice, _ := gf.Find(n)
	openClosePoint := [2]int{-1, -1}
	fmt.Println(s)
	if s != "" {
		for i, v := range nSlice.Data {
			fmt.Println(v)
			if v == s {
				openClosePoint[0] = i
			}
		}
	}
	if e != "" {
		for j := len(nSlice.Data) - 1; j < 0; j-- {
			if nSlice.Data[j] == e {
				openClosePoint[1] = j
			}
		}
	}
	newIndex := gf.Index
	if openClosePoint[0] >= 0 {
		newIndex = newIndex[openClosePoint[0]:]
	}
	if openClosePoint[1] >= 0 {
		newIndex = newIndex[:openClosePoint[1]+1]
	}
	fmt.Println(openClosePoint)
	fmt.Println(newIndex)
}

func (gf *GoFrame) FindIndex(s string) int {
	for i := 0; i < len(gf.Columns); i++ {
		if gf.Columns[i] == s {
			return i
		}
	}
	return -1
}

func (gf *GoFrame) FromIndexSlice(sl []int) []Series {
	newS := make([]Series, len(gf.Columns))
	for i := 0; i < len(newS); i++ {
		for j := 0; j < len(sl); j++ {
			newS[i].Data = append(newS[i].Data, gf.Data[i].Data[sl[j]])
		}
		for j := 0; j < len(newS[i].Data); j++ {
			newS[i].Index = append(newS[i].Index, j)
		}
	}
	return newS
}

func (gf *GoFrame) SelectRow(s string, ss string) *GoFrame {
	newG := GoFrame{Columns: make([]string, len(gf.Columns))}
	copy(newG.Columns, gf.Columns)
	sli, _ := gf.Find(s)
	var sameIndex []int
	for i := 0; i < len(sli.Index); i++ {
		if sli.Data[i].(string) == ss {
			sameIndex = append(sameIndex, i)
		}
	}
	newG.Data = gf.FromIndexSlice(sameIndex)
	newG.Index = make([]int, len(newG.Data[0].Index))
	copy(newG.Index, newG.Data[0].Index)
	return &newG
}

// 所有用到的函数

func IsNumber(str string) bool {
	for _, char := range str {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func ReadCsvFile(filepath string) ([][]string, error) {
	fileContent, err := os.Open(filepath + ".csv")
	if err != nil {
		return [][]string{}, err
	}
	defer fileContent.Close()
	lines, err := csv.NewReader(fileContent).ReadAll()
	if err != nil {
		return [][]string{}, err
	}
	return lines, nil
}

func ErrorExit(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func CsvToFrame(csvData [][]string) *GoFrame {
	gf := GoFrame{}
	ifIndex := false
	nlen := 0
	for i := 0; i < len(csvData); i++ {
		if i == 0 {
			if csvData[i][0] == "" {
				nlen = len(csvData[i]) - 1
				gf.Data = make([]Series, nlen)
				gf.Columns = csvData[i][1:]
				ifIndex = true
			} else {
				nlen = len(csvData[i])
				gf.Data = make([]Series, nlen)
				gf.Columns = csvData[i]
			}
		} else {
			gf.Index = append(gf.Index, i-1)
			if ifIndex {
				newCsv := csvData[i][1:]
				for j := 0; j < len(newCsv); j++ {

					gf.Data[j].AppendRow(newCsv[j])

				}
			} else {
				for j := 0; j < len(csvData[i]); j++ {
					gf.Data[j].AppendRow(csvData[i][j])
				}
			}
		}
	}
	return &gf
}

func DeepCopy(l *Series, y *Series) *Series {
	l.Index = make([]int, len(y.Index))
	copy(l.Index, y.Index)
	l.Data = make([]interface{}, len(y.Index))
	copy(l.Data, y.Data)
	return l
}

func Reverse(s []float64) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func MapToSlice(m map[string]bool) ([]string, []string) {
	var k []string
	var v []string

	for i, j := range m {
		k = append(k, i)
		v = append(v, strconv.FormatBool(j))
	}

	return k, v
}

func NetToCsv(n *[]Net, m int, p string) {
	s := strconv.FormatInt(int64(m), 10)
	file, err := os.Create(p + "combination_" + s + ".csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Write([]string{"因子", "方向", "净值"})
	for i := 0; i < len(*n); i++ {
		f := (*n)[i].F
		d := (*n)[i].D
		net := (*n)[i].N
		F := strings.Join(f, ",")
		D := strings.Join(d, ",")
		NET := strconv.FormatFloat(net, 'f', 'f', 32)
		writer.Write([]string{F, D, NET})
		writer.Flush()

	}
}
