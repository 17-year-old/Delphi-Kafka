object frmConsumer: TfrmConsumer
  Left = 0
  Top = 0
  Caption = #25910#28040#24687#27979#35797
  ClientHeight = 318
  ClientWidth = 411
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  Position = poScreenCenter
  OnClose = FormClose
  PixelsPerInch = 96
  TextHeight = 13
  object Label1: TLabel
    Left = 38
    Top = 20
    Width = 23
    Height = 13
    Caption = 'topic'
  end
  object Label2: TLabel
    Left = 21
    Top = 51
    Width = 42
    Height = 13
    Caption = 'message'
  end
  object btnStart: TButton
    Left = 101
    Top = 263
    Width = 75
    Height = 25
    Caption = #24320#22987#25509#25910
    TabOrder = 0
    OnClick = btnStartClick
  end
  object mmomsg: TMemo
    Left = 69
    Top = 51
    Width = 289
    Height = 193
    TabOrder = 1
  end
  object edtTopic: TEdit
    Left = 69
    Top = 16
    Width = 289
    Height = 21
    TabOrder = 2
    Text = 'test'
  end
  object btnStop: TButton
    Left = 238
    Top = 263
    Width = 75
    Height = 25
    Caption = #20572#27490
    Enabled = False
    TabOrder = 3
    OnClick = btnStopClick
  end
end
