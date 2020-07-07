unit uKafkaUtils;

interface

uses
  Windows, SysUtils, uLibKafka, Classes;

type
  { EKafkaError }

  EKafkaError = class(Exception)
  end;

  TOnKafkaMessageReceived = procedure(aMsg: prd_kafka_message_t) of object;

  TOnKafkaTick = procedure of object;

  { TKafkaClass }

  TKafkaClass = class
  private
    FKafkaConf: prd_kafka_conf_t;
    FTopicConf: prd_kafka_topic_conf_t;
    FKafka: prd_kafka_t;
  public
    procedure SetKafkaConfig(aKafkaConfig: TStrings); overload; // 设置全局的conf
    function SetKafkaConfig(aProp: string; aValue: string): rd_kafka_conf_res_t; overload;
    procedure SetTopicConfig(aTopicConfig: TStrings); overload; // 设置Topic级别的conf
    function SetTopicConfig(aProp: string; aValue: string): rd_kafka_conf_res_t; overload;
    function GetKafkaErrorCode: rd_kafka_resp_err_t;
    function GetKafkaErrorText(aErrorCode: rd_kafka_resp_err_t): string;
    procedure RaiseKafkaErrorIfExists(aErrNo: rd_kafka_resp_err_t);
    procedure CreateKafkaConf; // 创建全局的conf
    procedure CreateTopicConf; // 创建Topic级别的conf
    procedure SetKafkaDefaultTopicConf;
  end;

  { TKafkaConsumer }

  TKafkaConsumer = class(TKafkaClass)
  private
    FStop: Boolean;
    FTopicPartitionList: prd_kafka_topic_partition_list_t;
    FOnKafkaMessageReceived: TOnKafkaMessageReceived;
    FOnKafkaTick: TOnKafkaTick;
    procedure CreatePartitionList;
    procedure SubscribeTopic(Topics: TStrings);
    procedure AddTopic(aTopicName: string);
    procedure CreateConsumer;
    procedure SetConsumer;
  public
    // 消息接收是个死循环，除非停止收消息否则不会返回
    // 所以需要给程序的其他部分执行的机会
    // 这个回调的目的就是不论有没有收到消息，都会执行其代码
    // 不然停止程序、响应UI消息就不好实现
    // 具体可查看StartConsumer的代码
    property OnKafkaTick: TOnKafkaTick read FOnKafkaTick write FOnKafkaTick;
    // 收到消息后会调用这个函数
    // 这个回调会传入原始消息prd_kafka_message_t
    // 可通过函数 GetKafkaMessageAsString 取出字符串格式的消息
    property OnKafkaMessageReceived: TOnKafkaMessageReceived read FOnKafkaMessageReceived write FOnKafkaMessageReceived;
    // 接收消息
    // 必须指定服务器地址
    // 通过在aKafkaConfig中传入属性bootstrap.servers
    // 必须指定group.id
    // 相同group.id的消费者只有一个能收到消息？
    // 可以在aTopicConfigs中传入Topic级别的属性
    // Topics表示要接收哪些Topic
    constructor Create(aConfig: TStrings; aTopicConfig: TStrings; Topics: TStrings);
    procedure StartConsumer;
    procedure StopConsumer;
    destructor Destroy; override;
  end;

  { TKafkaProducer }

  TKafkaProducer = class(TKafkaClass)
  private
    procedure CreateProducer;
    function CreateTopic(aTopicName: string): prd_kafka_topic_t;
    procedure SetDeliveryReportCallback(aDeliveryReportCallBack: dr_msg_cb); // 设置消息发送状态回调函数
  public
    constructor Create; overload;

    // 要发送消息
    // 必须指定服务器地址
    // 可以通过在aKafkaConfig中传入属性bootstrap.servers
    // 或可以设置属性metadata.broker.list
    // 或调用函数rd_kafka_brokers_add
    // 消息发送是异步的？所以发送消息时可以传入回调函数以判断发送消息的状态
    constructor Create(aKafkaConfig: TStrings; aTopicConfig: TStrings; aDeliveryReportCallBack: dr_msg_cb); overload;
    procedure AddBrokers(Brokerlist: string);

    // 发送消息必须要指定topic
    // 可通过调用函数rd_kafka_topic_new生成
    // 发消息时还可以额外指定一个KEY和一个msg_opaque
    // KEY会和消息一起发送到服务器
    // msg_opaque会在回调函数dr_msg_cb中传回来,让应用程序判断消息发送的状态
    // 消息内容实际可以是任意二进制内容,不限于字符串
    // 一般需要应用程序自己根据Topic去解析消息
    // 这里只简单封装了发字符串的消息
    procedure ProduceMessage(aTopic: string; aMsg: string); overload;
    procedure ProduceMessage(aTopic: string; aKey, aMsg: string); overload;
    procedure ProduceMessage(aTopic: string; aKey, aMsg: string; msg_opaque: Pointer); overload;
    destructor Destroy; override;
  end;

  // 获取字符串格式的消息
function GetKafkaMessageAsString(aKafkaMessage: prd_kafka_message_t): string;

// 获取字符串格式的Key
function GetKafkaKeyAsString(aKafkaMessage: prd_kafka_message_t): string;

implementation

function GetKafkaMessageAsString(aKafkaMessage: prd_kafka_message_t): string;
begin
  Result := '';
  if aKafkaMessage^.len > 0 then
  begin
    SetString(Result, PAnsiChar(aKafkaMessage^.payload), aKafkaMessage^.len);
  end;
end;

function GetKafkaKeyAsString(aKafkaMessage: prd_kafka_message_t): string;
begin
  Result := '';
  if aKafkaMessage^.key_len > 0 then
  begin
    SetString(Result, PAnsiChar(aKafkaMessage^.key), aKafkaMessage^.key_len);
  end;
end;

{ TKafkaClass }

procedure TKafkaClass.SetKafkaDefaultTopicConf;
begin
  rd_kafka_conf_set_default_topic_conf(FKafkaConf, FTopicConf);
end;

procedure TKafkaClass.CreateKafkaConf;
begin
  FKafkaConf := rd_kafka_conf_new();
  if not Assigned(FKafkaConf) then
  begin
    EKafkaError.Create('rd_kafka_conf_new failed');
  end;
end;

procedure TKafkaClass.CreateTopicConf;
begin
  FTopicConf := rd_kafka_topic_conf_new();
  if not Assigned(FTopicConf) then
  begin
    EKafkaError.Create('rd_kafka_topic_conf_new failed');
  end;
end;

function TKafkaClass.GetKafkaErrorCode: rd_kafka_resp_err_t;
begin
  Result := rd_kafka_last_error;
end;

function TKafkaClass.GetKafkaErrorText(aErrorCode: rd_kafka_resp_err_t): string;
begin
  Result := '';
  if aErrorCode <> RD_KAFKA_RESP_ERR_NO_ERROR then
  begin
    Result := string(AnsiString(rd_kafka_err2name(aErrorCode)));
  end;
end;

procedure TKafkaClass.RaiseKafkaErrorIfExists(aErrNo: rd_kafka_resp_err_t);
var
  aErrMsg: string;
begin
  if aErrNo <> RD_KAFKA_RESP_ERR_NO_ERROR then
  begin
    aErrMsg := string(rd_kafka_err2name(aErrNo));
    raise EKafkaError.Create(aErrMsg);
  end;
end;

function TKafkaClass.SetKafkaConfig(aProp, aValue: string): rd_kafka_conf_res_t;
var
  errstr: array [0 .. 512] of AnsiChar;
  errstr_size: SIZE_T;
begin
  errstr := '';
  errstr_size := SizeOf(errstr);
  Result := rd_kafka_conf_set(FKafkaConf, PAnsiChar(AnsiString(aProp)), PAnsiChar(AnsiString(aValue)),
    PAnsiChar(@errstr), errstr_size);
  if Result = RD_KAFKA_CONF_UNKNOWN then
  begin
    raise EKafkaError.Create('RD_KAFKA_CONF_UNKNOWN');
  end
  else if Result = RD_KAFKA_CONF_INVALID then
  begin
    raise EKafkaError.Create('RD_KAFKA_CONF_INVALID');
  end;
end;

procedure TKafkaClass.SetKafkaConfig(aKafkaConfig: TStrings);
var
  i: Integer;
  PropKey, PropValue: string;
begin
  if Assigned(aKafkaConfig) then
  begin
    for i := 0 to aKafkaConfig.Count - 1 do
    begin
      PropKey := aKafkaConfig.Names[i];
      PropValue := aKafkaConfig.ValueFromIndex[i];
      if (PropKey <> '') and (PropValue <> '') and (PropKey[1] <> '#') then
      begin
        SetKafkaConfig(PropKey, PropValue);
      end;
    end;
  end;
end;

function TKafkaClass.SetTopicConfig(aProp, aValue: string): rd_kafka_conf_res_t;
var
  errstr: array [0 .. 512] of AnsiChar;
  errstr_size: SIZE_T;
begin
  errstr := '';
  errstr_size := SizeOf(errstr);
  Result := rd_kafka_topic_conf_set(FTopicConf, PAnsiChar(AnsiString(aProp)), PAnsiChar(AnsiString(aValue)),
    PAnsiChar(@errstr), errstr_size);
  if Result = RD_KAFKA_CONF_UNKNOWN then
  begin
    raise EKafkaError.Create('RD_KAFKA_CONF_UNKNOWN');
  end
  else if Result = RD_KAFKA_CONF_INVALID then
  begin
    raise EKafkaError.Create('RD_KAFKA_CONF_INVALID');
  end;
end;

procedure TKafkaClass.SetTopicConfig(aTopicConfig: TStrings);
var
  i: Integer;
  PropKey, PropValue: string;
begin
  if Assigned(aTopicConfig) then
  begin
    for i := 0 to aTopicConfig.Count - 1 do
    begin
      PropKey := aTopicConfig.Names[i];
      PropValue := aTopicConfig.ValueFromIndex[i];
      if (PropKey <> '') and (PropValue <> '') and (PropKey[1] <> '#') then
      begin
        SetTopicConfig(PropKey, PropValue);
      end;
    end;
  end;
end;

{ TKafkaConsumer }

constructor TKafkaConsumer.Create(aConfig: TStrings; aTopicConfig: TStrings; Topics: TStrings);
begin
  inherited Create;

  CreateKafkaConf;
  CreateTopicConf;
  SetKafkaConfig(aConfig);
  SetTopicConfig(aTopicConfig);
  SetKafkaDefaultTopicConf;

  CreateConsumer;
  SetConsumer;

  CreatePartitionList;
  SubscribeTopic(Topics);
end;

procedure TKafkaConsumer.CreateConsumer;
var
  errstr: array [0 .. 512] of AnsiChar;
  errstr_size: SIZE_T;
begin
  errstr := '';
  errstr_size := SizeOf(errstr);
  FKafka := rd_kafka_new(RD_KAFKA_CONSUMER, FKafkaConf, PAnsiChar(@errstr), errstr_size);
  if not Assigned(FKafka) then
  begin
    EKafkaError.Create(string(AnsiString(errstr)));
  end;
end;

procedure TKafkaConsumer.AddTopic(aTopicName: string);
begin
  rd_kafka_topic_partition_list_add(FTopicPartitionList, PAnsiChar(AnsiString(aTopicName)), -1);
  if not Assigned(FTopicPartitionList) then
  begin
    EKafkaError.Create('rd_kafka_topic_partition_list_add  failed');
  end;
end;

procedure TKafkaConsumer.CreatePartitionList;
begin
  FTopicPartitionList := rd_kafka_topic_partition_list_new(1);
  if not Assigned(FTopicPartitionList) then
  begin
    EKafkaError.Create('rd_kafka_topic_partition_list_new  failed');
  end;
end;

destructor TKafkaConsumer.Destroy;
begin
  rd_kafka_topic_partition_list_destroy(FTopicPartitionList);
  rd_kafka_consumer_close(FKafka);
  rd_kafka_destroy(FKafka);
  inherited;
end;

procedure TKafkaConsumer.SetConsumer;
var
  Result: rd_kafka_resp_err_t;
begin
  Result := rd_kafka_poll_set_consumer(FKafka);
  RaiseKafkaErrorIfExists(Result);
end;

procedure TKafkaConsumer.StopConsumer;
begin
  FStop := True;
end;

procedure TKafkaConsumer.SubscribeTopic(Topics: TStrings);
var
  i: Integer;
  Result: rd_kafka_resp_err_t;
begin
  for i := 0 to Topics.Count - 1 do
  begin
    AddTopic(Topics[i]);
  end;

  Result := rd_kafka_subscribe(FKafka, FTopicPartitionList);
  RaiseKafkaErrorIfExists(Result);
end;

procedure TKafkaConsumer.StartConsumer;
var
  aKafkaMessage: prd_kafka_message_t;
begin
  FStop := False;

  while True do
  begin
    if FStop = True then
    begin
      Break;
    end;

    if Assigned(FOnKafkaTick) then
    begin
      FOnKafkaTick;
    end;

    try
      aKafkaMessage := rd_kafka_consumer_poll(FKafka, 100);
      if not Assigned(aKafkaMessage) then
      begin
        continue;
      end;

      if Assigned(aKafkaMessage) then
      begin
        if aKafkaMessage^.err = RD_KAFKA_RESP_ERR_NO_ERROR then
        begin
          if Assigned(FOnKafkaMessageReceived) then
          begin
            FOnKafkaMessageReceived(aKafkaMessage);
          end;
        end;

        rd_kafka_message_destroy(aKafkaMessage);
      end;
    except
      // 接收和处理某个消息时异常不能中断主循环
    end;
  end;
end;

{ TKafkaProducer }

procedure TKafkaProducer.AddBrokers(Brokerlist: string);
var
  added: Int32;
begin
  added := rd_kafka_brokers_add(FKafka, PAnsiChar(AnsiString(Brokerlist)));
  if added = 0 then
  begin
    EKafkaError.Create('rd_kafka_brokers_add  failed');
  end;
end;

destructor TKafkaProducer.Destroy;
begin
  rd_kafka_destroy(FKafka);
  inherited;
end;

constructor TKafkaProducer.Create(aKafkaConfig: TStrings; aTopicConfig: TStrings; aDeliveryReportCallBack: dr_msg_cb);
begin
  inherited Create;

  CreateKafkaConf;
  SetKafkaConfig(aKafkaConfig);
  SetDeliveryReportCallback(aDeliveryReportCallBack);

  CreateTopicConf;
  SetTopicConfig(aTopicConfig);

  SetKafkaDefaultTopicConf;

  CreateProducer;
end;

constructor TKafkaProducer.Create;
begin
  Create(nil, nil, nil);
end;

procedure TKafkaProducer.CreateProducer;
var
  errstr: array [0 .. 512] of AnsiChar;
  errstr_size: Int32;
begin
  errstr := '';
  errstr_size := SizeOf(errstr);
  FKafka := nil;
  FKafka := rd_kafka_new(RD_KAFKA_PRODUCER, FKafkaConf, PAnsiChar(@errstr), errstr_size);
  if not Assigned(FKafka) then
  begin
    EKafkaError.Create('rd_kafka_new failed');
  end;
end;

function TKafkaProducer.CreateTopic(aTopicName: string): prd_kafka_topic_t;
begin
  Result := rd_kafka_topic_new(FKafka, PAnsiChar(AnsiString(aTopicName)), nil);
  if not Assigned(Result) then
  begin
    RaiseKafkaErrorIfExists(GetKafkaErrorCode);
  end;
end;

procedure TKafkaProducer.ProduceMessage(aTopic, aMsg: string);
begin
  ProduceMessage(aTopic, '', aMsg);
end;

procedure TKafkaProducer.ProduceMessage(aTopic, aKey, aMsg: string);
begin
  ProduceMessage(aTopic, aKey, aMsg, nil);
end;

procedure TKafkaProducer.ProduceMessage(aTopic: string; aKey, aMsg: string; msg_opaque: Pointer);
var
  topic: prd_kafka_topic_t;
  MessageLen, KeyLen: Integer;
  tmpKey, tmpMsg: AnsiString;
  errno: Integer;
  err: rd_kafka_resp_err_t;
label
  retry;
begin
  topic := CreateTopic(aTopic);
  try
    tmpKey := AnsiString(aKey);
    KeyLen := Length(tmpKey);
    tmpMsg := AnsiString(aMsg);
    MessageLen := Length(tmpMsg);

  retry:
    errno := rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, @tmpMsg[1], MessageLen, @tmpKey[1],
      KeyLen, msg_opaque);
    // 这里返回值和其他函数不太一样，参看uLibkafka或rdkafka.h的函数注释
    if errno <> 0 then
    begin
      err := rd_kafka_errno2err(errno);
      if err <> RD_KAFKA_RESP_ERR_NO_ERROR then
      begin
        // 发送队列已满，需要稍后重试
        if (err = RD_KAFKA_RESP_ERR__QUEUE_FULL) then
        begin
          rd_kafka_poll(FKafka, 1000);
          goto retry;
        end;
      end;
    end;

    rd_kafka_flush(FKafka, 60 * 1000);
  finally
    rd_kafka_topic_destroy(topic);
  end;
end;

procedure TKafkaProducer.SetDeliveryReportCallback(aDeliveryReportCallBack: dr_msg_cb);
begin
  if Assigned(aDeliveryReportCallBack) then
  begin
    rd_kafka_conf_set_dr_msg_cb(FKafkaConf, aDeliveryReportCallBack);
  end;
end;

end.
