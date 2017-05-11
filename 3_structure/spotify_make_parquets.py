#!/usr/bin/python3
from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode

class structParq(object):
    def __init__(self):
        self.app_name = 'Spotify Structure Parquet'
        self.conf = SparkConf().setAppName(self.app_name)
        self.conf = self.conf.setMaster("local[*]")
        self.sc = SparkContext(conf=self.conf)
        self.spark = SparkSession(self.sc)
        self.create_datetime_as_str = datetime.now().strftime('/%Y/%m/%d/%H/%M')
        self.parquet_base_path = "s3a://spotifybuck/output/parquet/"
        self.files_base_path = "s3a://spotifybuck/"
        self.files_date_path = (datetime.now() - timedelta(days=1)).strftime('/%Y/%m/%d/*/*')

    def explode_entities(self, inEntityType=1):
        if inEntityType == 1:
            self.raw_alb = self.spark.read.json(self.files_base_path + "albums" + self.files_date_path)
            self.exploded_alb = self.raw_alb.select(explode(self.raw_alb.albums).alias("album")).cache()
        elif inEntityType == 2:
            self.exploded_trk = self.exploded_alb.select(
                                  self.exploded_alb.album.id.alias("album_id"),
                                  explode(self.exploded_alb.album.tracks.items).alias("track"))
            self.exploded_alb.unpersist()
            self.exploded_trk.cache()
        else:
            self.exploded_trk.unpersist()
            self.raw_af = self.spark.read.json(self.files_base_path + "audiofeatures" + self.files_date_path)

    def write_parquet(self, inEntityName, inSparkDf):
        inSparkDf.write.parquet(self.parquet_base_path + inEntityName + self.create_datetime_as_str)

    def albums(self):
        alb = self.exploded_alb.selectExpr(
                      "album.id AS media_id",
                      "album.href",
                      "album.label",
                      "album.popularity",
                      "album.release_date",
                      "album.name",
                      "album.uri"
                ).dropDuplicates(["media_id"])
        self.write_parquet('albums', alb)

    def album_genres(self):
        exploded_genres = self.exploded_alb.select(
                                  self.exploded_alb.album.id.alias("media_id"),
                                  explode(self.exploded_alb.album.genres).alias("genre"))
        alb_gr = exploded_genres.selectExpr(
                      "media_id",
                      "genre",
                      "'album' AS media_type"
                ).dropDuplicates()
        self.write_parquet('media_genres/albums', alb_gr)

    def album_images(self):
        exploded_alb_im = self.exploded_alb.select(
                                  self.exploded_alb.album.id.alias("media_id"),
                                  explode(self.exploded_alb.album.images).alias("image"))
        alb_im = exploded_alb_im.selectExpr(
                      "media_id",
                      "image.height",
                      "image.width",
                      "image.url",
                      "'album' AS media_type"
                ).dropDuplicates(["media_id", "media_type"])
        self.write_parquet('media_images/albums', alb_im)

    def album_markets(self):
        exploded_alb_mkt = self.exploded_alb.select(
                                  self.exploded_alb.album.id.alias("media_id"),
                                  explode(self.exploded_alb.album.available_markets).alias("market"))
        alb_mkt = exploded_alb_mkt.selectExpr(
                      "media_id",
                      "market",
                      "'album' AS media_type"
                ).dropDuplicates()
        self.write_parquet('media_available_markets/albums', alb_mkt)

    def album_artists(self):
        exploded_alb_ar = self.exploded_alb.select(
                                  self.exploded_alb.album.id.alias("media_id"),
                                  explode(self.exploded_alb.album.artists.id).alias("artist_id"))
        alb_ar = exploded_alb_ar.selectExpr(
                      "media_id",
                      "artist_id",
                      "'album' AS media_type"
                ).dropDuplicates()
        self.write_parquet('media_artists/albums', alb_ar)

    def album_tracks(self):
        exploded_alb_tr = self.exploded_trk.select(
                                  self.exploded_trk.album_id,
                                  self.exploded_trk.track.id.alias("track_id"))
        alb_tr = exploded_alb_tr.selectExpr(
                      "album_id",
                      "track_id"
                ).dropDuplicates()
        self.write_parquet('album_tracks', alb_tr)

    def track_markets(self):
        exploded_trk_mkt = self.exploded_trk.select(
                                  self.exploded_trk.track.id.alias("media_id"),
                                  explode(self.exploded_trk.track.available_markets).alias("market"))
        trk_mkt = exploded_trk_mkt.selectExpr(
                      "media_id",
                      "market",
                      "'track' AS media_type"
                ).dropDuplicates()
        self.write_parquet('media_available_markets/tracks', trk_mkt)

    def track_artists(self):
        exploded_trk_ar = self.exploded_trk.select(
                                  self.exploded_trk.track.id.alias("media_id"),
                                  explode(self.exploded_trk.track.artists.id).alias("artist_id"))
        trk_ar = exploded_trk_ar.selectExpr(
                      "media_id",
                      "artist_id",
                      "'track' AS media_type"
                ).dropDuplicates()
        self.write_parquet('media_artists/tracks', trk_ar)

    def tracks(self):
        trk = self.exploded_trk.selectExpr(
                      "track.id AS media_id",
                      "track.name",
                      "track.disc_number",
                      "track.duration_ms",
                      "track.explicit",
                      "track.href",
                      "track.preview_url",
                      "track.track_number",
                      "track.uri"
                ).dropDuplicates(["media_id"])
        self.write_parquet('tracks', trk)

    def artists(self):
        exploded_trk_ar = self.exploded_trk.select(
                                  explode(self.exploded_trk.track.artists).alias("artist"))
        trk_ar = exploded_trk_ar.selectExpr(
                      "artist.id AS artist_id",
                      "artist.name AS name",
                      "artist.href AS href",
                      "artist.uri AS uri"
                ).dropDuplicates(["artist_id"])

        #exploded_alb_ar = self.exploded_alb.select(
        #                          explode(self.exploded_alb.album.artists).alias("artist"))
        #alb_ar = exploded_alb_ar.selectExpr(
        #              "artist.id AS artist_id",
        #              "artist.name AS name",
        #              "artist.href AS href",
        #              "artist.uri AS uri"
        #        ).dropDuplicates(["artist_id"])

        #art = alb_ar.unionAll(trk_ar).dropDuplicates(["artist_id"])
        self.write_parquet('artists', trk_ar)

    def audio_features(self):
        aud_af = self.raw_af.selectExpr(
                      "id as media_id",
                      "acousticness",
                      "danceability",
                      "duration_ms",
                      "energy",
                      "instrumentalness",
                      "key",
                      "liveness",
                      "loudness",
                      "mode",
                      "speechiness",
                      "time_signature",
                      "valence"
                ).dropDuplicates(["media_id"])
        self.write_parquet('audio_features', aud_af)

def main():
    parq = structParq()
    parq.explode_entities(1)
    parq.albums()
    parq.album_genres()
    parq.album_images()
    parq.album_markets()
    parq.album_artists()
    parq.explode_entities(2)
    parq.album_tracks()
    parq.track_markets()
    parq.track_artists()
    parq.tracks()
    parq.artists()
    parq.explode_entities(3)
    parq.audio_features()

if __name__ == '__main__':
    main()
