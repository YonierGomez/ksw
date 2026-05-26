class Ksw < Formula
  desc "AI-powered interactive Kubernetes context switcher"
  homepage "https://github.com/YonierGomez/ksw"
  url "https://github.com/YonierGomez/ksw/archive/refs/tags/v1.7.2.tar.gz"
  sha256 "a67ecbec3b511d223758ea3b55f10206ac680ca511ad2e2cbf331e6088646a68"
  license "MIT"

  depends_on "go" => :build
  depends_on "kubernetes-cli"

  def install
    system "go", "build", "-ldflags", "-s -w", "-o", bin/"ksw", "."
  end

  test do
    assert_match "ksw v#{version}", shell_output("#{bin}/ksw -v")
  end
end
