#
# Be sure to run `pod lib lint NAME.podspec' to ensure this is a
# valid spec and remove all comments before submitting the spec.
#
# Any lines starting with a # are optional, but encouraged
#
# To learn more about a Podspec see http://guides.cocoapods.org/syntax/podspec.html
#

Pod::Spec.new do |s|
  s.name             = "liblowladb"
  s.version          = "0.1.0"
  s.summary          = "A synchronizing embedded client database based on MongoDB."
  s.homepage         = "https://github.com/lowla/liblowladb"
  s.license          = 'Apache 2.0'
  s.author           = { "Mark Dixon" => "mark_dixon@teamstudio.com" }
  s.source           = { :git => "https://github.com/lowla/liblowladb.git", :tag => s.version.to_s }

  s.ios.deployment_target = '6.0'
  s.osx.deployment_target = '10.8'

  s.requires_arc = true
  s.compiler_flags = '-DMONGO_USE_LONG_LONG_INT'
  s.prefix_header_contents = '#ifndef NDEBUG','#define SQLITE_DEBUG = 1','#endif'

  s.ios.source_files = 'src/**/*.*','platform-src/ios/**/*.*'
  s.osx.source_files = 'src/**/*.*','platform-src/osx/**/*.*'
  #s.resources = 'Pod/Assets/*.png'

  # s.public_header_files = 'Pod/Classes/**/*.h'
  # s.frameworks = 'UIKit', 'MapKit'
  # s.dependency 'AFNetworking', '~> 2.3'
end
