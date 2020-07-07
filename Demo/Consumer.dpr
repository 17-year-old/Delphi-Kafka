program Consumer;

uses
  Vcl.Forms,
  uConsumerMain in 'uConsumerMain.pas' {frmConsumer} ,
  uLibKafka in '..\uLibKafka.pas',
  uKafkaUtils in '..\uKafkaUtils.pas';

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmConsumer, frmConsumer);
  Application.Run;

end.
