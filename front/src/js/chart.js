let option = {
                title: {
                    text: 'kh001 每日质量',
                    subtext: '从上线到当天的每日合格率'
                },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    }
                },
                legend: {
                    show: false
                },
                grid: {
                    left: '3%',
                    right: '4%',
                    bottom: '3%',
                    containLabel: true
                },
                xAxis: {
                    type: 'category',
                    splitLine: {
                        show: false
                    },
                    axisTick: {
                        show: false
                    },
                    data: ['2022/12/27', '2022/12/28'],
                },
                yAxis: {
                    type: 'value',
                    axisTick: {
                        show: false
                    },
                    splitLine: {
                        show: false
                    },
                    axisLine: {
                        show: true
                    },
                    axisLabel: {
                        formatter: function (data) {
                            return data + '%'
                        }
                    },
                    min: 50
                },
                series: {
                    name: '合格率',
                    type: 'bar',
                    barWidth: 30,
                    label: {
                        show: true,
                        formatter:  '{c}%'
                    },
                    itemStyle: {
                        color: function (params) {
                            let colorList = ['red', 'green'];
                            return colorList[params.dataIndex]
                        }
                    },
                    markLine: {
                        lineStyle: {
                            color: 'rgb(23, 162, 184)'
                        },
                        silent: true,
                        symbol: 'none',
                        data: [
                            {
                                yAxis: 98,
                                label: {
                                    show: true,
                                    position: 'insideEndTop',
                                    formatter: `合格率目标\n98%`
                                }
                            }
                        ],
                    },
                    data: [92, 99],
                }
            };