FROM ruby:2.1-onbuild

CMD ["./dynamo_scaler.rb", "--config_file", "config.json"]