FROM ruby:2.1-onbuild

# forward the log from the script to the docker log collector
RUN ln -sf /dev/stdout /var/log/dynamo_scaler/dynamo_scaler.log

CMD ["./dynamo_scaler.rb", "--config_file", "config.json"]