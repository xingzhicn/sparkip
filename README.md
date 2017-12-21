# sparkip
ip query attribution spark implementation

# spark streaming 通过ip地址查询所在地（JAVA版）


-------------------

## 前言




>最近项目有一个用户地域分析的需求，现在知道ip字段，需要在spark中通过用户的ip查找归属地，我们这里将ip直接转换成对应城市的字符串

首先我们需要一份ip库，字段格式如下：

`1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
1.0.8.0|1.0.15.255|16779264|16781311|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
1.0.32.0|1.0.63.255|16785408|16793599|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178`


**关于定位在这里我们只需要精确到城市，所以只需要三个字段，ip地址起始值和结束值，还有对应的城市，startip、stopip、city，分别对应3、4、8**

字段| 对应列数
-------- | ---
startip| 3
stopip| 4
city     | 8


----------


## 准备工作

ip库是从淘宝买的，csdn下载地址放在末尾，首先将我们的文件上传到hdfs上
> su hdfs

> hadoop fs -put ip.txt /user/ip.txt


上传之后的/user/目录


![](http://img.blog.csdn.net/20171207153804197?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvcXEzOTIwMzk3NTc=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)


----------


## 代码时间

下面上代码，首先读取hdfs上ip表，因为ip表可能不全，所以去掉了空值的情况
```java
        // java spark context
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // spark streaming context
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.milliseconds(4000));
        ssc.checkpoint("/checkpoint");
        ... ...

        List<String> ipCollect = jsc.textFile("hdfs://master:8020/user/ip.txt").collect();
        List<String[]> ipList = new ArrayList<>();
        try {
            for (String line : ipCollect) {
                if (!"".equals(line)) {
                    String[] split = line.split("\\|");
                    if (split[2] != null && split[3] != null && split[7] != null) {
                        String[] ipSection = new String[3];
                        ipSection[0] = split[2];
                        ipSection[1] = split[3];
                        ipSection[2] = split[7];
                        ipList.add(ipSection);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
```
然后读取ip表中的数据，将ip表注册成广播

```java
filtered_data.foreachRDD(new VoidFunction2<JavaRDD<Map<String, Object>>, Time>() {
            @Override
            public void call(JavaRDD<Map<String, Object>> rdd, Time time) throws Exception {
                // Get or register the ip Broadcast
                final Broadcast<List<String[]>> ipArray = JavaWordIp.getInstance(new JavaSparkContext(rdd.context()), ipList);
                JavaRDD<Row> rowRDD = rdd.map(x -> {
                    try {
                        String ip = "ip";
                        if (x.get(ip) != null || IpUtil.isIpAddress(x.get(ip).toString())) {
                            Long l = IpUtil.ip2long(x.get(ip).toString());
                            String region = IpUtil.binarySearch(ipArray.value(), l);
                            x.put(ip, region);
                        }
                        return x;
                    } catch (Exception e) {
                        System.err.println("error process: AdJobService ipJavaRDD" + e.getMessage());
                        throw e;
                    }
                }).unpersist();
```
工具类在根目录下

## 下载链接
>ip表的数据
http://download.csdn.net/download/qq392039757/10151360


