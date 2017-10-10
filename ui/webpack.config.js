var webpack = require('webpack');
var HtmlWebpackPlugin = require('html-webpack-plugin');
module.exports = {

    entry: {
        'app':'./src/main.ts',
        'vendor':'./src/vendor.ts',
        'polyfills':'./src/polyfills.ts'
    },
    output: {
        // path: '/dist',
        filename: '[name].js'
    },
    module: {
        rules: [
          {
            test: /\.ts$/,
            loader: 'ts-loader'
          },
          {
            test: /\.html$/,
            loader: 'html-loader'
          },
          {
            test: /\.(png|jpe?g|gif|svg|woff|woff2|ttf|eot|ico)$/,
            loader: 'file-loader?name=assets/[name].[hash].[ext]'
          },
          {
            test: /\.css$/,
            loaders: 'style-loader!css-loader' 
          },
          {
            test: /\.css$/,
            loader: 'raw-loader'
          }
        ]
    },

    resolve: {
        extensions: ['.js', '.ts']
    },
    plugins: [
        new webpack.optimize.CommonsChunkPlugin({
          name: ['app', 'vendor', 'polyfills','vendor.js']
        }),
    
        new HtmlWebpackPlugin({
          template: 'src/index.html'
        }),

        new webpack.ProvidePlugin({
        $: 'jquery',
        jQuery: 'jquery',
        'window.jQuery': 'jquery',
        jquery :'jquery',
        Popper: ['popper.js', 'default'],
        // In case you imported plugins individually, you must also require them here:
        Util: "exports-loader?Util!bootstrap/js/dist/util",
        Dropdown: "exports-loader?Dropdown!bootstrap/js/dist/dropdown",
        echarts: "echarts"
      })
    ],
    
};
