import os
import pathlib
from multiprocessing import Pool
import time
start_time = time.time()

#open all files in ./books
file_names_lines = []
for file in os.listdir('./books'):
    with open('./books/' + file, 'r') as f:
        file_names_lines.append([file, f.readlines()])


def word_count_map(file):
    wc = {}
    with open(file, mode='r') as f:
        for line in f:
            for word in line.split():
                if word in wc:
                    wc[word] += 1
                else:
                    wc[word] = 1
    return wc

if __name__ == '__main__':
    wc_final = {}
    with Pool(3) as mexecutor:
        items = [f for f in pathlib.Path('books/').glob("*.txt")]
        for wc in mexecutor.imap_unordered(word_count_map, items):
            for w in wc:
                wc_final[w] = wc_final.get(w, 0) + wc[w]

    #print the top 10 words
    for word, count in sorted(wc_final.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(word, count)

    print("--- %s seconds ---" % (time.time() - start_time))
