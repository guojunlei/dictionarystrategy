package functions

import (
	. "dictionary_strategy/structs"
	"sort"
	"strconv"
	"time"
)

func DeleteUseless(g *GoFrame, f []string, r []string) *GoFrame {
	newColumn := append(r, f...)

	var newIndex []int

	for i := 0; i < len(newColumn); i++ {
		newIndex = append(newIndex, g.FindIndex(newColumn[i]))
	}
	nG := GoFrame{Columns: newColumn, Index: []int{}, Data: make([]Series, len(newIndex))}
	for i := 0; i < len(newIndex); i++ {
		DeepCopy(&nG.Data[i], &g.Data[newIndex[i]])
	}
	nG.Index = make([]int, len(nG.Data[0].Index))
	copy(nG.Index, nG.Data[0].Index)

	return &nG
}

func RemoveRepSlice(slc []interface{}, s string) []interface{} {
	var r []interface{}
	tempMap := map[interface{}]byte{}
	if s == "time" {
		for _, e := range slc {
			l := len(tempMap)
			tempMap[e] = 0
			if len(tempMap) != l {
				r = append(r, e)
			}
		}
	}
	return r
}

func CalculateCurve(g *GoFrame, m map[string]bool, n int, t string) Net {
	dateS, _ := g.Find("trade_date")
	timeS := RemoveRepSlice(dateS.Data, "time")

	var StrategyRet []Res
	for i := 0; i < len(timeS); i++ {
		data := g.SelectRow("trade_date", timeS[i].(string))
		for k, v := range m {
			factorS, _ := data.Find(k)
			data.Columns = append(data.Columns, k+"_rank")
			r := factorS.Rank(v)
			rS := Series{Index: data.Data[0].Index, Data: r}
			data.Data = append(data.Data, rS)
		}
		// rank 组合新因子
		factorS, _ := MapToSlice(m)
		finalRank := make([]float64, len(data.Index))
		for i := 0; i < len(factorS); i++ {
			d, _ := data.Find(factorS[i] + "_rank")
			for i, _ := range finalRank {
				a := float64(d.Data[i].(int))
				finalRank[i] += a
			}
		}
		f := make([]float64, len(finalRank))
		copy(f, finalRank)
		sort.Float64s(f)
		f = f[:n]
		var liveIndex []int
		for i := 0; i < len(f); i++ {
			for j := 0; j < len(finalRank); j++ {
				if f[i] == finalRank[j] {
					liveIndex = append(liveIndex, j)
				}
			}
		}
		var res Res
		res.T = timeS[i].(string)
		var ret float64
		for _, i := range liveIndex {
			j := data.FindIndex("next_return")
			r := data.Data[j]
			R, _ := strconv.ParseFloat(r.Data[i].(string), 64)
			ret += R
		}
		ret = ret / float64(len(liveIndex))
		res.Ret = ret
		StrategyRet = append(StrategyRet, res)
	}
	//计算累积净值
	net := AccumulatedNet(&StrategyRet, t)
	k, v := MapToSlice(m)
	return Net{F: k, D: v, N: net}
}

func AccumulatedNet(r *[]Res, t string) float64 {
	var net float64 = 1
	TT, _ := time.ParseInLocation("2006/1/2", t, time.Local)

	for i := 0; i < len(*r); i++ {
		tt, _ := time.ParseInLocation("2006-01-02", (*r)[i].T, time.Local)
		if tt.After(TT) {
			r := (*r)[i]
			net = net * (1 + r.Ret)
		}
	}
	return net
}

func DeleteMin(nA []Net, n Net) {
	netS := make([]float64, 0)
	for i := 0; i < len(nA); i++ {
		netS = append(netS, nA[i].N)
	}
	var m float64
	var ind int
	for i := 0; i < len(netS); i++ {
		if i == 0 {
			m = netS[i]
			ind = i
		} else if netS[i] < m {
			m = netS[i]
			ind = i
		}
	}
	if m < n.N {
		nA[ind].F = n.F
		nA[ind].D = n.D
		nA[ind].N = n.N
	}
}
