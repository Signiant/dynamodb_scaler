FROM ruby:2.1-onbuild

RUN chmod a+x ./dynamo_scaler.rb
CMD ["./dynamo_scaler.rb", "--overrides_file", "overrides.json"]
