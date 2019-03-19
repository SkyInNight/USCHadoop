# USCHadoop
Hadoop简单Maven项目搭建

#### 使用注意事项
1. 本项目的运行配置为为：
    + Java环境：jdk1.8（请务必是jdk1.8如果不是`请在pom.xml里面修改jdk.tools的版本号`，jkd11里面没有tools.jar请降低版本执行）
    + 运行系统：Manjora(Linux即可，最好使用Linux或MacOS运行此项目，`Windows可能会有权限错误`)
    + 运行软件：Idea(可以使用Eclipse替代)
2. 项目中需要将文件编译环境修改成和jdk相同的编译环境
    ![AuZGsH.png](https://s2.ax1x.com/2019/03/19/AuZGsH.png)
3. 此项目为控制台Application项目，所以需要配置Application启动项（这里仅演示Idea上的操作）
    + 选择添加运行项目
    ![AulBrT.png](https://s2.ax1x.com/2019/03/19/AulBrT.png)
    + 选择Application项目
    ![AulDqU.png](https://s2.ax1x.com/2019/03/19/AulDqU.png)
    + 配置Application的主函数参数
    ![AulsZF.png](https://s2.ax1x.com/2019/03/19/AulsZF.png)
    + **这里请务必务必添加上执行参数 input/ output/ 不然运行会报错**
    ![Aul0MV.png](https://s2.ax1x.com/2019/03/19/Aul0MV.png)
4. 项目运行完一次后会生成output/文件夹，如果需要二次运行，需要手动删除output文件夹