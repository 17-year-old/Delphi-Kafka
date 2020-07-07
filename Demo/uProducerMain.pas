unit uProducerMain;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, uKafkaUtils, uLibKafka, Vcl.StdCtrls;

type
  TfrmProducer = class(TForm)
    Button1: TButton;
    edtTopic: TEdit;
    Label1: TLabel;
    Label2: TLabel;
    mmoMsg: TMemo;
    edtKey: TEdit;
    Label3: TLabel;
    procedure Button1Click(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  frmProducer: TfrmProducer;

implementation

{$R *.dfm}

procedure TfrmProducer.Button1Click(Sender: TObject);
var
  aKafkaProducer: TKafkaProducer;
  aKafkaConfig: TStrings;
  aTopicConfig: TStrings;
begin
  aKafkaConfig := TStringList.Create;
  aTopicConfig := TStringList.Create;
  aKafkaConfig.Add('bootstrap.servers=192.168.1.169:9092');
  aKafkaProducer := TKafkaProducer.Create(aKafkaConfig, aTopicConfig, nil);

  aKafkaProducer.ProduceMessage(edtTopic.Text, Trim(edtKey.Text), Trim(mmoMsg.Text));
  aKafkaProducer.Free;
  aKafkaConfig.Free;
  aTopicConfig.Free;
end;

end.
