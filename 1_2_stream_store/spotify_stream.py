#!/usr/bin/python3
from json import dumps
from spotipy import oauth2, Spotify

class iSpotify(object):
    def __init__(self):
        self.token=None

    def connectSpotify(self, d_credentials, service_name):
        self.auth = oauth2.SpotifyClientCredentials(**d_credentials[service_name])
        self.token = self.auth.get_access_token()
        self.tk_spot = Spotify(auth=self.token)

    def regenerateTokenIfExpired(self):
        self.token = self.auth.get_access_token()
        self.tk_spot = Spotify(auth=self.token)

    def getCategories(self, country='US', locale='en_US', limit=50, offset=0):
        self.regenerateTokenIfExpired()
        self.categories = dumps(self.tk_spot.categories(country='US', locale='en_US', limit=20, offset=offset))

    def getCategoryNewReleases(self, cat_id, limit=50, country='US', offset=0):
        self.regenerateTokenIfExpired()
        self.category_new_releases = dumps(self.tk_spot.category_playlists(cat_id, limit=limit,country=country, offset=offset))

    def getUserPlayList(self, user_id, limit=50, offset=0):
        self.regenerateTokenIfExpired()
        self.user_playlists = dumps(self.tk_spot.user_playlists(user_id, limit=limit, offset=offset))

    def getPlaylistTracks(self, owner_id, playlist_id, limit=50, country='US', offset=0):
        self.regenerateTokenIfExpired()
        self.playlist_tracks = dumps(self.tk_spot.user_playlist_tracks(owner_id, playlist_id, limit=limit,market=country, offset=offset))

    def getAlbums(self, album_id=[]):
        self.regenerateTokenIfExpired()
        self.albums=dumps(self.tk_spot.albums(album_id))

    def getArtists(self, artist_id=[]):
        self.regenerateTokenIfExpired()
        self.artists = dumps(self.tk_spot.artists(artist_id))

    def getAudioFeatures(self, track_id):
        self.regenerateTokenIfExpired()
        self.audio_features = dumps(self.tk_spot.audio_features(track_id))

    def getAudioAnalysis(self, track_id):
        self.regenerateTokenIfExpired()
        self.audio_analysis = dumps(self.tk_spot.audio_analysis(track_id))


