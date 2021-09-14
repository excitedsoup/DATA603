def main():
    f = open('WarAndPeace.txt', 'r', encoding="utf8")
    text = f.read()
    f.close()
    
    punct = '!"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~—“”'
    
    for i in text:
        if i in punct:
            text = text.replace(i, ' ')
    
    splitText = text.split()
    
    for i in range(len(splitText)):
        splitText[i] = splitText[i].lower()
    
    uniqueWords = set(splitText)
    
    print('There are ' + str(len(uniqueWords)) + ' unique words in the provided text file of War and Peace')
main()
