describe('util/model', function() {

    var utHelper = window.utHelper;
    var modelUtil;

    beforeAll(function (done) { // jshint ignore:line
        utHelper.resetPackageLoader(function () {
            window.require(['echarts/util/model'], function (h) {
                modelUtil = h;
                done();
            });
        });
    });

    function makeRecords(result) {
        var o = {};
        modelUtil.eachAxisDim(function (dimNames) {
            o[dimNames.name] = {};
            var r = result[dimNames.name] || [];
            for (var i = 0; i < r.length; i++) {
                o[dimNames.name][r[i]] = true;
            }
        });
        return o;
    }

    describe('findLinkedNodes', function () {

        function forEachModel(models, callback) {
            for (var i = 0; i < models.length; i++) {
                callback(models[i]);
            }
        }

        function axisIndicesGetter(model, dimNames) {
            return model[dimNames.axisIndex];
        }

        it('findLinkedNodes_base', function (done) {
            var models = [
                {xAxisIndex: [1, 2], yAxisIndex: [0]},
                {xAxisIndex: [3], yAxisIndex: [1]},
                {xAxisIndex: [5], yAxisIndex: []},
                {xAxisIndex: [2, 5], yAxisIndex: []}
            ];
            var result = modelUtil.createLinkedNodesFinder(
                utHelper.curry(forEachModel, models),
                modelUtil.eachAxisDim,
                axisIndicesGetter
            )(models[0]);
            expect(result).toEqual({
                nodes: [models[0], models[3], models[2]],
                records: makeRecords({x: [1, 2, 5], y: [0]})
            });
            done();
        });

        it('findLinkedNodes_crossXY', function (done) {
            var models = [
                {xAxisIndex: [1, 2], yAxisIndex: [0]},
                {xAxisIndex: [3], yAxisIndex: [3, 0]},
                {xAxisIndex: [6, 3], yAxisIndex: [9]},
                {xAxisIndex: [5, 3], yAxisIndex: []},
                {xAxisIndex: [8], yAxisIndex: [4]}
            ];
            var result = modelUtil.createLinkedNodesFinder(
                utHelper.curry(forEachModel, models),
                modelUtil.eachAxisDim,
                axisIndicesGetter
            )(models[0]);
            expect(result).toEqual({
                nodes: [models[0], models[1], models[2], models[3]],
                records: makeRecords({x: [1, 2, 3, 5, 6], y: [0, 3, 9]})
            });
            done();
        });

        it('findLinkedNodes_emptySourceModel', function (done) {
            var models = [
                {xAxisIndex: [1, 2], yAxisIndex: [0]},
                {xAxisIndex: [3], yAxisIndex: [3, 0]},
                {xAxisIndex: [6, 3], yAxisIndex: [9]},
                {xAxisIndex: [5, 3], yAxisIndex: []},
                {xAxisIndex: [8], yAxisIndex: [4]}
            ];
            var result = modelUtil.createLinkedNodesFinder(
                utHelper.curry(forEachModel, models),
                modelUtil.eachAxisDim,
                axisIndicesGetter
            )();
            expect(result).toEqual({
                nodes: [],
                records: makeRecords({x: [], y: []})
            });
            done();
        });

    });

    describe('compressBatches', function () {

        function item(seriesId, dataIndex) {
            return {seriesId: seriesId, dataIndex: dataIndex};
        }

        it('base', function (done) {
            // Remove dupliate between A and B
            expect(modelUtil.compressBatches(
                [item(3, 4), item(3, 5), item(4, 5)],
                [item(4, 6), item(4, 5), item(3, 3), item(3, 4)]
            )).toEqual([
                [item('3', [5])],
                [item('3', [3]), item('4', [6])]
            ]);

            // Compress
            expect(modelUtil.compressBatches(
                [item(3, 4), item(3, 6), item(3, 5), item(4, 5)],
                [item(4, 6), item(4, 5), item(3, 3), item(3, 4), item(4, 7)]
            )).toEqual([
                [item('3', [5, 6])],
                [item('3', [3]), item('4', [6, 7])]
            ]);

            // Remove duplicate in themselves
            expect(modelUtil.compressBatches(
                [item(3, 4), item(3, 6), item(3, 5), item(4, 5)],
                [item(4, 6), item(4, 5), item(3, 3), item(3, 4), item(4, 7), item(4, 6)]
            )).toEqual([
                [item('3', [5, 6])],
                [item('3', [3]), item('4', [6, 7])]
            ]);

            // dataIndex is array
            expect(modelUtil.compressBatches(
                [item(3, [4, 5, 8]), item(4, 4), item(3, [5, 7, 7])],
                [item(3, [8, 9])]
            )).toEqual([
                [item('3', [4, 5, 7]), item('4', [4])],
                [item('3', [9])]
            ]);

            // empty
            expect(modelUtil.compressBatches(
                [item(3, [4, 5, 8]), item(4, 4), item(3, [5, 7, 7])],
                []
            )).toEqual([
                [item('3', [4, 5, 7, 8]), item('4', [4])],
                []
            ]);
            expect(modelUtil.compressBatches(
                [],
                [item(3, [4, 5, 8]), item(4, 4), item(3, [5, 7, 7])]
            )).toEqual([
                [],
                [item('3', [4, 5, 7, 8]), item('4', [4])]
            ]);

            // should not has empty array
            expect(modelUtil.compressBatches(
                [item(3, [4, 5, 8])],
                [item(3, [4, 5, 8])]
            )).toEqual([
                [],
                []
            ]);

            done();
        });

    });

});