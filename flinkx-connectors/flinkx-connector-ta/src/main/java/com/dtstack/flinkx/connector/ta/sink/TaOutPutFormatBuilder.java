package com.dtstack.flinkx.connector.ta.sink;

import com.dtstack.flinkx.connector.ta.conf.TaConf;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;

public class TaOutPutFormatBuilder extends BaseRichOutputFormatBuilder {

    private TaOutPutFormat format;

    public TaOutPutFormatBuilder() {
        super.format = format = new TaOutPutFormat();

    }

    public void setTaConf(TaConf taConf) {
        super.setConfig(taConf);
        format.setTaConf(taConf);

    }

    @Override
    protected void checkFormat() {

    }
}
