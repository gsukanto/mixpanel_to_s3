require 'aws-sdk-v1'
require 'net/http'
require 'csv'
require 'json'
require 'digest'
require 'zip/zip'
require 'dotenv'

Dotenv.load

def upload_to_s3(hash_data, file_name, destination_path, bucket_name)
    s3                = AWS::S3.new(
                        access_key_id: ENV['AWS_ACCESS_KEY_ID'],
                        secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
                      )
    bucket            = s3.buckets[bucket_name]
    
    begin
      object          = bucket.objects["#{destination_path}/#{file_name}"]
      url             = object.public_url
      puts            "="*80, "uploading #{file_name} to #{url}", "="*80
      object.write("#{hash_data}")
    rescue    Exception => e
      puts            "="*80, "[FAILED] to upload #{file_name} to #{destination_path}", "="*80
      raise   e
    end

    puts              "="*80, "Saved #{file_name} to aws #{url}", "="*80
  end

def write_to_csv(hash_data, file_name)
  puts              "="*80, "Writing #{file_name} CSV...", "="*80
  
  CSV.open("/tmp/#{file_name}", "w") do |csv|
    headers         = hash_data.map {|row| row["properties"].keys.map(&:to_s) }.flatten.uniq
    csv             << ["event"] + headers
    hash_data.each do |event|
      csv           << [event["event"]] + headers.map { |col|
        col         == "time" ? Time.at(event["properties"][col].to_i).strftime("%Y-%m-%d %H:%M:%S") : event["properties"][col].to_s
      }
    end
  end

  return File.open("/tmp/#{file_name}", 'r').read
end

def get_mixpanel_log(from_date, to_date)
  puts              "="*80, "Sending GET request to Mixpanel API...", "="*80
  api_secret        = ENV['MIXPANEL_API_SECRET']
  params            = "api_key=#{ENV['MIXPANEL_API_KEY']}&expire=#{Time.now.to_i + 600}&from_date=#{from_date}&to_date=#{to_date}"

  sig               = Digest::MD5.hexdigest(params.gsub(/&/, "") + api_secret)
  puts              "="*80, "Params: #{params}&sig=#{sig}", "="*80
  uri               = URI.parse("http://data.mixpanel.com/api/2.0/export?#{params}&sig=#{sig}")
  req               = Net::HTTP::Get.new(uri.to_s)
  res               = Net::HTTP.start(uri.host, uri.port) {|http| http.request(req) }

  puts              "="*80, "Get data completed...", "="*80
  json              = "[#{res.body.gsub(/\n/, ", ")[0..-3]}]"
  hash_data         = JSON.parse(json)
  
  return hash_data
end

def zip_variable(hash_data, file_name)
  puts              "="*80, "Compressing file...", "="*80

  stringio          = Zip::OutputStream.write_buffer do |zio|
    zio.put_next_entry(file_name)
    zio.write "#{hash_data}"
  end

  stringio.rewind
  return stringio.sysread
end

def main
    file_name         = ENV['FILE_NAME'].to_s
    destination       = ENV['DESTINATION_PATH'].to_s
    from_date         = ENV['FROM_DATE']
    to_date           = ENV['TO_DATE']

    if file_name == '' or destination == ''
      puts 'USAGE:'
      puts '    ruby mixpanel_to_s3.rb /path/to/your_file_name (mandatory) bucket_name/bucket/folder (mandatory) start_date (optional) end_date (optional) CSV=true (optional) COMPRESS=true (optional)'
      puts 'example:'
      puts '    ruby mixpanel_to_s3.rb FILE_NAME=mixpanel DESTINATION_PATH=devbuck/tmp COMPRESS=true'
      puts '    ruby mixpanel_to_s3.rb FILE_NAME=mixpanel DESTINATION_PATH=devbuck/tmp CSV=true'
      puts '    ruby mixpanel_to_s3.rb FILE_NAME=mixpanel DESTINATION_PATH=devbuck/tmp FROM_DATE=2015-09-15 TO_DATE=2015-09-16 COMPRESS=true'
      exit(1)
    end

    splitted          = destination.split('/')
    if splitted.count < 2
      puts 'DESTINATION_PATH=bucket_name/destination_folder'
      exit(1)
    end

    bucket_name       = splitted.first
    destination_path  = splitted[1..-1].join('/')
    puts destination_path
    one_day           = 1
    unless from_date
      from_date       = (Time.now - one_day).strftime("%Y-%m-%d")
    end

    unless to_date
      to_date         = (Time.now - one_day).strftime("%Y-%m-%d")
    end

    temp_date         = from_date

    while temp_date <= to_date
      temp_file_name  = "#{file_name}_#{temp_date}.log"

      hash_data       = get_mixpanel_log(temp_date, temp_date)
      if ENV['CSV']
        temp_file_name = "#{file_name}_#{temp_date}.csv"
        hash_data     = write_to_csv(hash_data, temp_file_name)
        system        "rm /tmp/#{temp_file_name}"
      end

      if ENV['COMPRESS']
        hash_data     = zip_variable(hash_data, temp_file_name)
        temp_file_name = "#{temp_file_name}.zip"
      end

      upload_to_s3(hash_data, temp_file_name, destination_path, bucket_name)

      temp_date       = (Date.parse(temp_date) + one_day).strftime("%Y-%m-%d")
    end

end

main()