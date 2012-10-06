#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

# MongoDBからSolrにデータを流しこむ

require 'rubygems'
require 'mongo'
require 'rsolr'

EXPORT_COL = "images" #エクスポート対象のコレクション名
SOLR_URL = "http://localhost:8080/solr"

if ARGV.size < 1
  puts "Usage: #{$0} DB_NAME [option]"
  puts "--delete Delete all indexes"
  exit(0)
end

# --deleteオプションが付いていたら、Solr上の全indexを削除する
if ARGV[1] == "--delete"
  puts "Are you sure to delete all indexes? [y,n]"
  answer = STDIN.gets
  if answer == "y\n" || answer == "yes\n"
    solr = RSolr.connect :url => SOLR_URL
    solr.delete_by_query "*:*"
    solr.commit
    puts "Deleted all indexes"
  end
  exit(0)
end

def build_solr_doc(record)
  record.delete("keyword") #Solr上ではいらないので削除
  record["_id"] = record["_id"].to_s #元はBSONなので

  record.merge!(record["catalog"]) #catalogはネストされてるので展開
  record.delete("catalog")

  record
end

@solr = RSolr.connect :url => SOLR_URL
@mongo = Mongo::Connection.new.db(ARGV[0])

#公開状態のもの(目録読み込みと画像変換が終了)だけ検索
#将来的にはopenLevel(資料公開範囲)や
#invisible(目録公開可否)も考慮したほうがいいかも
condition = {
  "catalog" => {"$exists" => true},
  "path" => {"$exists" => true}
}
solr_docs = Array.new
@mongo[EXPORT_COL].find(condition, {:timeout => false}) do |c|
  c.each do |record|
    solr_docs << build_solr_doc(record)
  end
end

@solr.add solr_docs
@solr.commit
