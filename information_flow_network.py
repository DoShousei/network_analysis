import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt


def read_tweets():
    """
    读取twitter生成user列表
    :return: user列表
    """
    user_tweet_list = []
    with open('Tweets/R_DeodorantCancer.txt') as f2:
        for line, column in enumerate(f2):
            column = column.replace('\n', '')
            user_t_id, tweet_id, content, time = column.split('\t')[:]
            user_tweet_list.append(user_t_id)
    user_tweet_list = list(map(int, user_tweet_list))
    user_tweet_list = set(user_tweet_list)  # 去除重复用户id
    print("tweet_read_complete")
    return user_tweet_list


def read_links():
    """

    :return: data frame; user_id, following_id
    """
    reader = pd.read_csv('deodorant_link_network.csv', header=None, names=['user_id', 'following_id'], iterator=True)
    loop = True
    chunkSize = 50
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    order = ['following_id', 'user_id']
    df = df[order]
    return df


if __name__ == '__main__':
    user_list = read_tweets()
    df_links = read_links()
    link_list = df_links.values.tolist()  # 按行转换成列表
    G = nx.DiGraph()
    G.add_nodes_from(user_list)
    G.add_edges_from(link_list)
    # print(G.number_of_nodes())
    # print(G.number_of_edges())
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, font_size=5, node_size=30, width=0.3, alpha=0.8)
    plt.show()
