list_of_file_names = []
word_count = dict()
#open all files in ./books
import os
import time
start_time = time.time()

for file in os.listdir('./books'):
    with open('./books/' + file, 'r') as f:
        list_of_file_names.append(file)

#count the number of words overall
for file in list_of_file_names:
    with open('./books/' + file, 'r') as f:
        for line in f:
            for word in line.split():
                if word in word_count:
                    word_count[word] += 1
                else:
                    word_count[word] = 1

#print the top 10 words
for word, count in sorted(word_count.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(word, count)

print("--- %s seconds ---" % (time.time() - start_time))