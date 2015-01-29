package com.ck;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class MySplitFunction extends BaseFunction {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: 自定义split
   */
  private static final long serialVersionUID = 1L;

  private String _splitFlag;

  public MySplitFunction() {}

  public MySplitFunction(String splitFlag) {
    this._splitFlag = splitFlag;
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    String tupleStr = tuple.getString(0);
    if (tupleStr != null) {
      if (_splitFlag != null && _splitFlag.equals("#")) {
        String[] strArr = tupleStr.split(_splitFlag);
        collector.emit(new Values(strArr[3]));
      } else {// 当drpc调用时，使用空格
        String[] strArr = tupleStr.split(_splitFlag);
        for (int i = 0; i < strArr.length; i++) {
          collector.emit(new Values(strArr[i]));
        }
      }

    }
  }

  public String get_splitFlag() {
    return _splitFlag;
  }

  public void set_splitFlag(String _splitFlag) {
    this._splitFlag = _splitFlag;
  }
}
