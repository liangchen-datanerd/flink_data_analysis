# US-Population-Analysis-with-Flink

## 1. Experiment Environment
- WSL
- Integrated Development Environment: IntelliJ IDEA 2019.3 (Ultimate Edition)
- Apache Maven: 3.8.1
- Python: 3.8.7
- Apache Flink: 1.11.2

## 2. Dataset
The dataset used for this project is the "US County Information" dataset, which consists of 3094 rows of information. Each row contains six attributes:
- County: Name of the county in the United States, e.g., Autauga County, Baldwin County, etc.
- State: Corresponding state to which the county belongs, e.g., Alabama, Alaska, etc.
- FIPS Code: Federal Information Processing Standards (FIPS) code, a numeric code designated by the National Institute of Standards and Technology (NIST).
- Population: Population count of the county.
- Area: Land area of the county in square miles.
- Density: Population density of the county, calculated as population divided by area (people per square mile).

The specific data format is as follows:

Download Link: [US County Data on GitHub](https://github.com/balsama/us_counties_data)

## 3. Data Processing and Analysis using Apache Flink
To facilitate data storage and processing, we first define a case class to store the data from the dataset:

```scala
// Define a case class to store the dataset
// Fields include county name, state name, FIPS code, population count, area, and density
case class CountyInfo(county: String, state: String, FIPS: String, population: Int, area: Int, density: Int)
```

### 3.1. Calculating the County with the Largest Area in a State
To calculate the county with the largest area in a specific state, we follow these steps:
1. Filter the data to keep only the records for the desired state (e.g., Alaska) using the `filter` operator.
2. Remove unnecessary fields (e.g., FIPS code, population) using the `map` operator.
3. Use the `maxBy` operator to determine the county with the largest area.
4. Extract the county name using the `map` operator.

In this example, the county with the largest area in Alaska is "Unorganized Borough," which is consistent with the information obtained from the Wikipedia page.

Code:

```scala
  /**
   * calculate which county have largest area for a state
   * @param StateName
   * @param inputPath
   */
  def getMaxAreaCountyInState(StateName: String, inputPath: String) = {

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[countyInfo](inputPath, ignoreFirstLine = true)

    val county = input.filter(_.state == StateName) // filter state
      .map(x => (x.area, x.county, x.state)) //keep useful colunms
      .maxBy(0)
      .map(x => x._2)


    println(s"max area for $StateName is ")
    county.print()
  }
```

### 3.2. Calculating Total Population, Total Area, and Population Density for Each State
To calculate the total population, total area, and population density for each state, we follow these steps:
1. Remove unnecessary fields from the dataset (e.g., FIPS code) using the `map` operator.
2. Group the data by state using the `groupBy` operator.
3. Calculate the total population and total area for each state using the `reduce` operator.
4. Calculate the population density for each state by dividing the total population by the total area, rounding to two decimal places, using the `map` operator.

#### Code:
```scala
  /**
   * calculate  total population total area and population density for every state
   * @param inputPath
   * @param outPut
   */
  def getStateInfo(inputPath: String, outPut: String) = {
    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[countyInfo](inputPath, ignoreFirstLine = true)

    val stateInfo = input.map(x => (x.population, x.area, x.state)) //keep useful colunms
      .groupBy(2) // aggregate by state
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3))
      .map((x) => (x._1, x._2, (x._1.asInstanceOf[Float] / x._2.asInstanceOf[Float]).formatted("%.2f"), x._3))
      .setParallelism(1)

    stateInfo.writeAsCsv(outPut+ "stateInfo.csv", writeMode=WriteMode.OVERWRITE)
    stateInfo.print()
  }
```

### 3.3. Calculating Top-10 States based on Statistical Data
To calculate the top-10 states based on statistical data, we follow these steps:
1. Read the results file (e.g., state.csv) obtained from the previous step (3.2).
2. Use the `map` operator to extract the required fields (e.g., area, population, or density) and state name.
3. Use the `sortPartition` operator to sort the data based on the desired field in descending order.
4. Use the `first` operator to retrieve the top-10 states.

#### Code:
```scala
  def getStateTop10(relativePath: String) = {

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment

    // data input
    val input = env.readCsvFile[(Int,Int,Float,String)](relativePath+ "stateInfo.csv", ignoreFirstLine = true)


    // calculate top 10 population
    val populationTop10 = input.map(x => (x._1, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)

    // calculate top 10 area
    val areaTop10 = input.map(x => (x._2, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)

    // calculate top 10 population density
    val desityTop10 = input.map(x => (x._3, x._4))
      .sortPartition(0, Order.DESCENDING)
      .setParallelism(1)
      .first(10)


    populationTop10.writeAsCsv(relativePath + "populationTop10.csv")
    populationTop10.print()
    areaTop10.writeAsCsv(relativePath + "areaTop10.csv")
    areaTop10.print()
    desityTop10.writeAsCsv(relativePath + "densityTop10.csv")
    desityTop10.print()

  }
```

## 4. Data Visualization

### 4.1. Bar Chart of the Top 10 States in Population in the United States

```python
def drawTop10PopulationState(inUrl):

    #read data from csv file
    df = pd.read_csv(inUrl,names=['population','state'])
    population = df['population'].tolist()
    state = df['state'].tolist()

    #plot set x,y axis
    plt.xlabel("population")
    plt.ylabel("state")
    plt.barh(state,population,facecolor='tan',height=0.5,edgecolor='r',alpha=0.6)
    plt.title("top 10 population for each state in US")
    plt.show()
```

![populationTop10.png](https://s2.loli.net/2023/07/14/QvcsPz3N6KYuEMd.png)

### 4.2. Word Cloud of States in the United States based on Area

```python
def drawAreaWordCloud(inUrl):
    #1 read data from file
    df = pd.read_csv(inUrl,names=['population','area','density','state'])
    area = df['area'].tolist()
    state = df['state'].tolist()
    #2.generate dict info
    dict = {}
    for i in range(len(state)):
        dict[state[i]]=area[i]
    #3.generate word cloud
    wc = wordcloud.WordCloud(
        background_color='white',
        width=800,
        height=600,
        max_font_size=50,
        min_font_size=10,
        max_words=1000 )
    wc.generate_from_frequencies(dict)
    wc.to_file('../output/word_cloud.jpg')
```

![word_cloud.jpg](https://s2.loli.net/2023/07/14/917wkgdejEQ3vzO.jpg)

### 4.3. Pie Chart of the Top 10 States in Population Density in the United States

```python
def drawDensityPie(inUrl):
    # read data from file
    df = pd.read_csv(inUrl,names=['density','state'])
    density = df['density'].tolist()
    state = df['state'].tolist()
    #画图
    plt.axes(aspect=1)
    plt.pie(x=density,labels=state,autopct="%0f%%",shadow=True)
    plt.title("Top 10 density state in US")
    plt.show()
```

![pie.png](https://s2.loli.net/2023/07/14/2fR4AZKlsewmNE9.png)