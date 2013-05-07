#!/usr/bin/env ruby
#
# To run this, first install nokogiri: "gem install nokogiri"

require 'pp'
require 'set'

require 'rubygems'
require 'nokogiri'
require 'open-uri'

NAMESPACE = 'http://maven.apache.org/POM/4.0.0'

doc = Nokogiri::XML(open("pom.xml"))

profile_to_symbols = {}
symbol_to_profiles = {}
all_profile_ids = []

profiles = doc.xpath('//x:project/x:profiles/x:profile', 'x' => NAMESPACE)
profiles.each do |profile|
  munge_symbols = profile.xpath('x:properties/x:munge.symbols', 'x' => NAMESPACE)
  profile_id = profile.xpath('x:id', 'x' => NAMESPACE).text
  all_profile_ids << profile_id
  munge_symbols.text.split(',').each do |munge_symbol|
    profile_to_symbols[profile_id] ||= Set.new
    profile_to_symbols[profile_id] << munge_symbol
    symbol_to_profiles[munge_symbol] ||= Set.new
    symbol_to_profiles[munge_symbol] << profile_id
  end
end

max_length = symbol_to_profiles.keys.map { |x| x.length }.max
puts " " * (max_length+2) + all_profile_ids.join(' | ')
symbol_to_profiles.each_pair do |munge_symbol, profile_ids|
  print "%-#{max_length}s |" % munge_symbol
  all_profile_ids.each do |check_profile_id|
    half_length = check_profile_id.length / 2
    space = " " * half_length
    print space
    if profile_ids.include?(check_profile_id)
      print "x"
    else
      print " "
    end
    print " " if check_profile_id.length.odd?
    print "#{space}| "
  end
  puts ""
end
