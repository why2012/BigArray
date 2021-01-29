## BigArray
## Large file processing tool

## 1、从海量IP黑名单引出BigArray

​	IP黑名单实现目的是从百万、上亿的IP库中，快速找出目标IP，从而快速判断目标IP是否是需要过滤的IP。由于一个IP占用4个字节，因此总量有2^32个，大约43亿个，因此将大量IP以字符串形式存储，无法实现快速高效检索的目的。

​	一种比较常用的方案是使用ip mask，将每个IP表示为byte数组中的一位（bit），那么对于2^32个IP，只需要2^32位（2^29 byte）即可完全表示，即需要512MB的存储空间，就可以存下整个ipmask。

​	使用一个IP（4字节）的高29位来定位IP所属的字节，使用低3位来定位IP在此字节中的位置，即表示此IP的那一个bit在字节中的位置。具体寻找IP位的方法如下：

```java
byte[] ipmask = new byte[512 * 1024 * 1024]; // 用于存储所有IP位
int ip = 78; // 表示目标IP，需要确认此IP有无在ipmask中
byte ipByte = ipmask[ip >>> 3]; // 找出IP所在的字节
int bitIndicator = 1 << (ip & 7); // 计算出IP在此字节中的bit位置
boolean isBlackIp = ipByte & bitIndicator != 0; // 判断此IP位是否被置为1
```

​	使用此种方案能够在O(1)时间复杂度内查找到目标IP。就目前所有服务器配置来讲，可以完全存下512MB的ipmask，但需要考虑到的是，判断目标IP是否在ipmask中只是业务程序的部分功能，为了这个功能占用512MB内存并不合算。因此需要考虑将ipmask放置在磁盘上，然后按需加载所需字节块，既能保证查询速度，又能节省内存。

​	除此之外，还需要考虑稀疏IP库的情况。假设IP库中的IP分布比较稀疏，512MB大小的数组实际使用比例很低，大部分空间存储数值0，大量内存空间被浪费。

​	BigArray即是这样一个虚拟的字节数组，以页面（MappedPage）形式组织数据，只加载需要的页面，其余页面放置于磁盘。从使用者视角，使用BigArray可以申请远超内存的字节数组，并且其分页机制可以存储稀疏数据，无关分页将不会被创建。

## 2、BigArray内存模型

-> 0  -> 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8 -> 9

如上所示，假设需要存储10个字节，内存可用大小只有2个字节，那么将无法一次将这些数据保存在原生数组里。BigArray使用了分页与子分页的概念解决此问题。

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;page0&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;page1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;page2

&nbsp;+---------------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------------+

&nbsp;&nbsp;&nbsp;&nbsp;+---------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------+

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp; 0，1 &nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;subpage0&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;4，5&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;8,&nbsp;&nbsp;9&nbsp;&nbsp;&nbsp;|

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp; 2，3 &nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;subpage1&nbsp;&nbsp;&nbsp;---->&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;6，7&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;---->               

&nbsp;&nbsp;&nbsp;&nbsp;+---------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------+

&nbsp;+---------------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------------+&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;+---------------+

在存储如上数据的BigArray里，有3个分页，第一二个分页包含2个子分页，第三个分页包含1个子分页。每个分页4字节，每个子分页2字节。若需要访问序号为4的数据，BigArray会加载page1的第一个子分页, 分页序号1（4 / 4 = 1），子分页序号0 (4  % 4 = 0)。若之后需要访问序号为2的数据，BigArray会先换出page1，之后加载page0的第二个子分页。

如此能保证在有限内存里，使用远超内存大小的数组。API使用方式如下：

```java
String dir = ".\\data";

BigArray bigArray = new BigArray.Builder(dir).pageSizeInBytes(4).
    maxPageInMem(3).subPageSizeInBytes(2).maxSubPageInMem(1).build();

for (int i = 0; i < 10; i++) {
    bigArray.putByte(i, (byte)i);
}

bigArray.getByte(4);
bigArray.getByte(2);

bigArray.close()
```

若需要存储序号为100的字节，BigArray不会将0 - 25号分页全部创建出来，而只会创建第25号分页的第一个子分页，仅占用2字节空间。

如上只是示例，在目前16G内存的机器上，使用BigArray理论上可以虚拟出磁盘可用大小的数组，而实际占用内存大小只与被加载入内存的子分页大小有关。

BigArray详细使用方式请参考Bootstrap内的示例代码。BigArray除了支持数据存取，还支持外部排序（K路归并），与从数据库存取分页数据。

更多基于BigArray的上层数据结构将会陆续更新。