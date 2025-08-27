function Charts() {

}

Charts.prototype.listenAttributeChange = function () {
    // tab 链接
    let targetNode = document.getElementById('custom-tabs-three-progress');
    // MutationObserver 的观察内容 attribute
    let config = {attributes: true};
    // 1. chart 的容器 div
    let progressDom1 = document.getElementById('product-progress');
    let productionDom1 = document.getElementById('product-production');
    let qualityDom1 = document.getElementById('product-quality');

    let callback = function (mutationList, observer) {
        // 如果 targetNode 的class 包含了 show
        if(targetNode.className.indexOf('show') > -1){
            // 2. 初始化 echarts
            let progressChart1 = echarts.init(progressDom1);
            let productionChart1 = echarts.init(productionDom1);
            let qualityChart1 = echarts.init(qualityDom1);
            // 3. 定义每个 echarts 对象的 option
            let progressOption1 = {
                title: {
                    text: 'kh001生产进度',
                    subtext: '计划 vs. 实际'
                },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    }
                },
                legend: {
                    right: '10%',
                    show: false
                },
                grid: {
                    left: '3%',
                    right: '4%',
                    bottom: '3%',
                    containLabel: true
                },
                xAxis: {
                    type: 'value',
                    splitLine: {
                        show: false
                    }
                },
                yAxis: {
                    type: 'category',
                    axisTick: {
                        show: false
                    },
                    data: ['实际\n\n2022/12/27上线', '计划\n\n2022/12/25上线'],
                    // max: 100
                },
                series: [
                    {
                        name: '完成',
                        type: 'bar',
                        barWidth: 30,
                        stack: 'total',
                        label: {
                            show: true
                        },
                        emphasis: {
                            focus: 'series'
                        },
                        itemStyle: {
                            color: function (params) {
                                let colorList = ['red', 'rgb(23, 162, 184)'];
                                return colorList[params.dataIndex];
                            }
                        },
                        data: [1200, 3200]
                    },
                    {
                        name: '未完成',
                        type: 'bar',
                        barWidth: 30,
                        stack: 'total',
                        label: {
                            show: true,
                        },
                        emphasis: {
                            focus: 'series'
                        },
                        itemStyle: {
                            color: '#ccc'
                        },
                        data: [3800, 1800],
                    }
                ]
            };
            let productionOption1 = {
                title: {
                    text: 'kh001 每日产量',
                    subtext: '从上线到当天的每日产量'
                },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: {
                        type: 'shadow'
                    }
                },
                legend: {
                    right: '10%',
                    show: false
                },
                grid: {
                    left: '3%',
                    right: '10%',
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
                    data: ['2022/12/27', '2022/12/28']
                },
                yAxis: {
                    type: 'value',
                    splitLine: {
                        show: false
                    },
                    axisLine: {
                        show: true
                    },
                    splitNumber: 4,
                },
                series: [
                    {
                        name: '日产量',
                        type: 'bar',
                        barWidth: 30,
                        label: {
                            show: true
                        },
                        emphasis: {
                            focus: 'series'
                        },
                        itemStyle: {
                            color: 'red'
                        },
                        data: [500, 700],
                        markLine: {
                            lineStyle: {
                                color: 'rgb(23, 162, 184)'
                            },
                            silent: true,
                            symbol: 'none',
                            label: {
                                show: true,
                                position: 'insideEndTop',
                                formatter: '日产量目标\n800'
                            },
                            data: [
                                {
                                    yAxis: 800
                                }
                            ]
                        }
                    }
                ]
            };
            let qualityOption = {
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
            // 4. 定义窗口变化时, echarts 对象随之调整大小
            window.onresize = function () {
                progressChart1.resize();
                productionChart1.resize();
                qualityChart1.resize();
            };
            // 5. 定义图表
            progressChart1.setOption(progressOption1);
            productionChart1.setOption(productionOption1);
            qualityChart1.setOption(qualityOption);
        }else{
        // 断开 MutationObserver
        }
    };

    let observer = new MutationObserver(callback);

    observer.observe(targetNode, config);


};

Charts.prototype.run = function () {
    let self = this;

    self.listenAttributeChange();
};

$(function () {
    let charts = new Charts();

    charts.run();
});