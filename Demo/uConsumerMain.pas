unit uConsumerMain;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants,
  System.Classes, Vcl.Graphics, Vcl.Controls, Vcl.Forms, Vcl.Dialogs,
  uKafkaUtils, uLibKafka, Vcl.StdCtrls;

type
  TfrmConsumer = class(TForm)
    btnStart: TButton;
    edtTopic: TEdit;
    Label1: TLabel;
    Label2: TLabel;
    mmomsg: TMemo;
    btnStop: TButton;
    procedure btnStartClick(Sender: TObject);
    procedure btnStopClick(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    { Private declarations }
    FKafkaConsumer: TKafkaConsumer;
  public
    { Public declarations }
    procedure OnKafkaMessageReceived(aMsg: prd_kafka_message_t);
    procedure OnKafkaTick;
  end;

var
  frmConsumer: TfrmConsumer;

implementation

{$R *.dfm}

procedure TfrmConsumer.btnStartClick(Sender: TObject);
var
  aConfig: TStrings;
  aTopicConfig: TStrings;
  Topics: TStrings;
begin
  aConfig := TStringList.Create;
  // 必须设置
  aConfig.Add('bootstrap.servers=192.168.1.169:9092');
  // 必须设置, 相同group.id的消费者只有一个能收到消息
  aConfig.Add('group.id=testgroup');

  aTopicConfig := TStringList.Create;

  Topics := TStringList.Create;
  Topics.Add(edtTopic.Text);

  FKafkaConsumer := TKafkaConsumer.Create(aConfig, aTopicConfig, Topics);
  FKafkaConsumer.OnKafkaTick := OnKafkaTick;
  FKafkaConsumer.OnKafkaMessageReceived := OnKafkaMessageReceived;
  btnStart.Enabled := False;
  btnStop.Enabled := True;
  Application.ProcessMessages;
  FKafkaConsumer.StartConsumer;
  FKafkaConsumer.Free;

  Topics.Free;
  aTopicConfig.Free;
  aConfig.Free;
end;

procedure TfrmConsumer.btnStopClick(Sender: TObject);
begin
  FKafkaConsumer.StopConsumer;
  btnStart.Enabled := True;
  btnStop.Enabled := False;
end;

procedure TfrmConsumer.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  if Assigned(FKafkaConsumer) then
    FKafkaConsumer.StopConsumer;
end;

procedure TfrmConsumer.OnKafkaMessageReceived(aMsg: prd_kafka_message_t);
begin
  mmomsg.Lines.Add(GetKafkaMessageAsString(aMsg));
  Application.ProcessMessages;
end;

procedure TfrmConsumer.OnKafkaTick;
begin
  Application.ProcessMessages;
end;

end.

