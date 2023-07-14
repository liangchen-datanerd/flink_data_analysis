import pandas as pd
import matplotlib.pyplot as plt
import wordcloud

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


# drawTop10PopulationState('../target/classes/populationTop10.csv')
# drawAreaWordCloud('../target/classes/stateInfo.csv')
drawDensityPie('../target/classes/densityTop10.csv')
