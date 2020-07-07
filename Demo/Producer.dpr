program Producer;

uses
  Vcl.Forms,
  uProducerMain in 'uProducerMain.pas' {frmProducer} ,
  uLibKafka in '..\uLibKafka.pas',
  uKafkaUtils in '..\uKafkaUtils.pas';

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmProducer, frmProducer);
  Application.Run;

end.
