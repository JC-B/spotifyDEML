#!/usr/bin/env python
# k-Nearest Neighbor algorithm
import math
import pandas as pd
import random
from sklearn.metrics import confusion_matrix
import pylab as pl
import operator

def loadData(trainSet, testSet, split):
    rawData = pd.read_csv('../../data/out.csv', sep='|')
    data_array = rawData.values
    data_list = list(data_array)
    #traverse the entire data_list
    for k in range(len(data_list)-1):
        #for each attribute
        for i in range(38):
            data_list[k][i] = float(data_list[k][i])
        #randomly split the data into two sets; with the ratio of sets = to split
        if random.uniform(0.0, 1.0) < split:
            trainSet.append(data_list[k])
        else:
            testSet.append(data_list[k])

def distanceE(value1, value2, length):
    d = 0
    #return the euclidean distance
    for i in range(length):
        diff = (value1[i] - value2[i])
        d = d + pow(diff, 2)
    return math.sqrt(d)

def getNearestNeighbor(trainSet, testValue, k):
    d_array = []
    testLength = len(testValue) - 1
    trainSetLength = len(trainSet)
    
    #append distances from each attribute to d_array
    #sort the array and initialize neigbour
    for i in range(trainSetLength):
        currDist = distanceE(testValue, trainSet[i], testLength)
        d_array.append((trainSet[i], currDist))
    d_array.sort(key = operator.itemgetter(1))
    neighbor = []
    
    #append to neighbors array, the k-nearest neighbors
    for i in range(k):
        neighbor.append(d_array[i][0])
    return neighbor

def getVotes(neighbors):
    votes = {}
    neigborLength = len(neighbors)
    #traverse k-nearest neighbors
    for i in range(neigborLength):
        hit = neighbors[i][-1]
        #if a value from neighbors is in votes, increase the vote for that value by 1
        if hit in votes:
            votes[hit] = votes[hit] + 1
        else:
        #keep the vote the same otherwise
            votes[hit] = 1
    #sort the array
    votesSorted = sorted(votes.items(), key = operator.itemgetter(1), reverse = True)
    
    #return the neighbor with the highest amount of votes
    return votesSorted[0][0]

def findAcc(testSet, predictions):
    c = 0
    writeFile = open("results.txt", 'wb')
    testSetLength = len(testSet)
    for i in range(testSetLength):
        #writeFile.write( "expected {0} got {1}\n".format(testSet[i][-1], predictions[i]))
        print(testSet[i][-1], predictions[i])
        if testSet[i][-1] == predictions[i]:
            c = c + 1
    result = (c/float(len(testSet)))
    print(result)
    writeFile.write("percent accuracy {0}".format(result))
    writeFile.close()
    return result * 100

    
def main():
    trainSet = []
    testSet = []
    y_pred = []
    y_true = []
    predicted = []
    mySplit = 0.67
    loadData(trainSet, testSet, mySplit)

    testSetLength = len(testSet)
    trainSetLength = len(trainSet)

    myK = 13 
    print ('Size of TRAIN set = ' + repr(trainSetLength))
    print ('Size of TEST set = ' + repr(testSetLength))
    for i in range(testSetLength):
        nearestNeighbors = getNearestNeighbor(trainSet, testSet[i], myK)
        result = getVotes(nearestNeighbors)
        predicted.append(result)
        y_pred.append(repr(result))
        y_true.append(repr(testSet[i][-1]))
    accuracy = findAcc(testSet, predicted)
    
    acc = float(repr(accuracy))
    print("Predicted accuracy: %.4f" % acc + " %")
    cm = confusion_matrix(y_true, y_pred)
    fig = pl.figure()
    ax = fig.add_subplot(111)
    cax = ax.matshow(cm)
    fig.colorbar(cax)
    #labels = ["analysis_sample_rate", "artist_familiarity", "artist_hotttnesss", "artist_latitude", "artist_longitude", "artist_terms", "artist_terms_freq", "artist_terms_weight", "bars_confidence", "bars_start", "beats_confidence", "beats_start", "danceability" ," duration", "end_of_fade_in", "energy", "key", "key_confidence", "loudness", "mode", "mode_confidence", "sections_confidence", "sections_start", "segments_confidence", "segments_loudness_max", "segments_loudness_max_time", "segments_loudness_start", "segments_pitches", "segments_start", "segments_timbre", "song_hotttnesss", "start_of_fade_out", "tatums_confidence", "tatums_start", "tempo" ,"time_signature", "time_signature_confidence", "year"]
    #ax.set_xticklabels([''] + labels)
    #ax.set_yticklabels([''] + labels)
    pl.title('Confusion matrix for K-Nearest Neighbor (without normalization)')
    pl.xlabel('Predicted')
    pl.ylabel('True')
    pl.show()
    pl.savefig('figure_8.png')
    
main()
