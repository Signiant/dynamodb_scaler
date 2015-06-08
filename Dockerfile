FROM ruby:2.1-onbuild

CMD ["./dynamo_scaler.rb", "--overrides_file", "overrides.json"]