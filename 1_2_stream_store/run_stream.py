#!/usr/bin/python3
from datetime import datetime
from functools import partial
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
from json import loads
from toolz.dicttoolz import get_in
from yaml import load
from aws_operations import awsOperations
from project_utils import utils
from spotify_stream import iSpotify


class spotifyProject(object):
    def __init__(self):
        self.curr_datetime_as_str = datetime.now().strftime('%Y/%m/%d/%H/%M/')
        self.return_code = 0

    def loadConstants(self):
        self.util = utils()
        self.util.setConstants()

    def loadYaml(self):
        self.cred = load(open(self.util.d_constants['credentials_file']))

    def setAwsUp(self):
        self.aws=awsOperations()
        self.aws.connectS3(self.cred, self.util.d_constants['service_aws'])

    def createS3Bucket(self):
        self.aws.createS3Bucket(self.util.d_constants['s3_bucket'], self.util.d_constants['aws_user_email'])

    def setSpotifyUp(self):
        self.spotify=iSpotify()
        self.spotify.connectSpotify(self.cred, self.util.d_constants['service_spotify'])

    def uploadCategories(self):
        self.spotify.getCategories(self.util.d_constants['country'], self.util.d_constants['locale'], self.util.d_constants['limit'], self.util.offset)
        self.aws.createS3Key('categories/' + self.curr_datetime_as_str + 'category', self.spotify.categories)
        self.util.writeSearchParams('catid', self.get_cat_ids())

    def uploadCategoryNewReleases(self, tup_cat_id):
        try:
            self.spotify.getCategoryNewReleases(tup_cat_id[1])
            self.aws.createS3Key('catnewreleases/' + self.curr_datetime_as_str + 'catnewrelease' + str(tup_cat_id[0] + 1), self.spotify.category_new_releases)
            SearchParams = [i+"~"+v for i, v in zip(self.get_rel_playlist_owner(), self.get_rel_playlist_id())]
            self.util.writeSearchParams('patt', SearchParams)
        except:
            self.util.logFailedSearch( "error_"+ self.curr_datetime_as_str, "Cat New Rel "+ tup_cat_id[1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))

    def poolCategoryNewReleases(self):
        cat_id = self.util.readSearchParams('catid')
        cat_id = [(i,v) for i, v in enumerate(cat_id)]
        pool = ThreadPool(processes=min(cpu_count(), len(cat_id)))
        pool.map(self.uploadCategoryNewReleases, cat_id)
        pool.close()

    def uploadPlaylistTracks(self, tup_p_att):
        try:
            self.spotify.getPlaylistTracks(tup_p_att[1][0], tup_p_att[1][1], self.util.d_constants['limit'], self.util.d_constants['country'], self.util.offset)
            self.aws.createS3Key('playlisttracks/' + self.curr_datetime_as_str + 'playlisttrack' + str(tup_p_att[0]+1), self.spotify.playlist_tracks)
            self.util.writeSearchParams('ptrack', self.get_rel_playlist_album_ids())
        except:
            self.util.logFailedSearch("error_" + self.curr_datetime_as_str, "Playlist Track " + tup_p_att[1][0] + tup_p_att[1][1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))

    def poolPlaylistTracks(self):
        p_att = self.util.readSearchParams('patt')
        p_att = [(i,v) for i, v in enumerate(p_att)]
        pool = ThreadPool(processes=min(cpu_count(), len(p_att)))
        pool.map(self.uploadPlaylistTracks, p_att)
        pool.close()

    def uploadAlbums(self, tup_al):
        try:
            self.spotify.getAlbums(tup_al[1])
            self.aws.createS3Key('albums/' + self.curr_datetime_as_str + 'album' + str(tup_al[0]+1), self.spotify.albums)
            self.util.writeSearchParams('album', self.get_rel_playlist_album_ids())
            search_params = self.get_album_artist_track_info()
            self.util.writeSearchParams('atrack', [i[0] for i in search_params])
            self.util.writeSearchParams('artist', [i[1] for i in search_params])
        except:
            self.util.logFailedSearch("error_" + self.curr_datetime_as_str, "Album " + tup_al[1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))

    def poolAlbums(self):
        al = self.util.readSearchParams('ptrack')
        sub_al = [(i, al[i:i + 10]) for i in range(0, len(al), 10)]
        pool = ThreadPool(processes=min(cpu_count(), len(sub_al)))
        pool.map(self.uploadAlbums, sub_al)
        pool.close()

    def uploadArtists(self, tup_ar):
        try:
            self.spotify.getArtists(tup_ar[1])
            try:
                self.spotify.getAudioAnalysis(tup_track_id[1])
                self.aws.createS3Key('audioanalysis/'+ self.curr_datetime_as_str + 'audioanalysis' + str(tup_track_id[0]+1), self.spotify.audio_analysis)
            except:
                self.util.logFailedSearch("error_" + self.curr_datetime_as_str, "Audio Analysis " + tup_track_id[1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))
            self.aws.createS3Key('artists/'+ self.curr_datetime_as_str + 'artist' + str(tup_ar[0]+1), self.spotify.artists)
        except:
            self.util.logFailedSearch("error_" + self.curr_datetime_as_str, "Artist " + tup_ar[1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))

    def poolArtists(self):
        ar = self.util.readSearchParams('artist')
        sub_ar = [(i, ar[i:i + 10]) for i in range(0, len(ar), 10)]
        pool = ThreadPool(processes=min(cpu_count(), len(sub_ar)))
        pool.map(self.uploadArtists, sub_ar)
        pool.close()

    def uploadAudioFeatures(self, tup_track_id):
        try:
            self.spotify.getAudioFeatures(tup_track_id[1])
            self.aws.createS3Key('audiofeatures/' + self.curr_datetime_as_str + 'audiofeature' + str(tup_track_id[0]+1), self.spotify.audio_features)
        except:
            self.util.logFailedSearch("error_" + self.curr_datetime_as_str, "Audio Feat " + tup_track_id[1] + datetime.now().strftime('%Y/%m/%d/%H/%M/%S'))

    def poolAudioFeatures(self):
        track_id = self.util.readSearchParams('atrack')
        track_id = [(i,v) for i, v in enumerate(track_id)]
        pool = ThreadPool(processes=min(cpu_count(), len(track_id)))
        pool.map(self.uploadAudioFeatures, track_id)
        pool.close()

    def uploadAudioAnalysis(self, tup_track_id):
        pass

    def poolAudioAnalysis(self):
        track_id = self.util.readSearchParams('atrack')
        track_id = [(i,v) for i, v in enumerate(track_id)]
        pool = ThreadPool(processes=min(cpu_count(), len(track_id)))
        pool.map(self.uploadAudioAnalysis, track_id)
        pool.close()

    def get_cat_ids(self):
        return map(partial(get_in, ['id']),
                   get_in(['categories', 'items'], loads(self.spotify.categories), []))

    def get_user_playlist_owner(self):
        return map(partial(get_in, ['owner', 'id']),
                   get_in(['items'], loads(self.spotify.category_new_releases), []))

    def get_user_playlist_id(self):
        return map(partial(get_in, ['id']),
                   get_in(['items'], loads(self.spotify.category_new_releases), []))

    def get_rel_playlist_album_ids(self):
        return map(partial(get_in, ['track', 'album', 'id']),
                   get_in(['items'], loads(self.spotify.playlist_tracks), []))

    def get_album_artist_track_info(self):
        return [(get_in(['id'], i[0], []), get_in(['id'], j, []))
                for i in map(partial(get_in, ['tracks', 'items']),
                             get_in(['albums'], loads(self.spotify.albums), []))
                for j in get_in(['artists'], i[0], [])]


def do_it_all():
    sp = spotifyProject()
    sp.loadConstants()
    sp.loadYaml()
    sp.setAwsUp()
    sp.createS3Bucket()
    sp.setSpotifyUp()
    with open(sp.util.homedir + 'user_playlist', 'r') as f:
            self.d_constants = dict(x.rstrip().split(":", 1) for x in f)

    sp.uploadCategories()
    sp.poolCategoryNewReleases()
    sp.poolPlaylistTracks()
    sp.poolAlbums()
    sp.poolArtists()
    sp.poolAudioFeatures()
    sp.poolAudioAnalysis()
    sp.util.updateOffset()
    sp.util.delSearchParams()


do_it_all()
