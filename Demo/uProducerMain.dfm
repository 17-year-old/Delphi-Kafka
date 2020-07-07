object frmProducer: TfrmProducer
  Left = 0
  Top = 0
  Caption = #21457#28040#24687#27979#35797
  ClientHeight = 359
  ClientWidth = 406
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  Position = poScreenCenter
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
    Left = 19
    Top = 77
    Width = 42
    Height = 13
    Caption = 'message'
  end
  object Label3: TLabel
    Left = 44
    Top = 47
    Width = 17
    Height = 13
    Caption = 'key'
  end
  object Button1: TButton
    Left = 163
    Top = 298
    Width = 75
    Height = 25
    Caption = #21457#36865
    TabOrder = 0
    OnClick = Button1Click
  end
  object mmoMsg: TMemo
    Left = 69
    Top = 77
    Width = 289
    Height = 193
    Lines.Strings = (
      'mmoMsg')
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
  object edtKey: TEdit
    Left = 69
    Top = 43
    Width = 289
    Height = 21
    TabOrder = 3
    Text = 'key'
  end
end
