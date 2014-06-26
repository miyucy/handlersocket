# -*- mode: ruby -*-
$script = <<SCRIPT
cat <<EOF > /etc/apt/sources.list
deb mirror://mirrors.ubuntu.com/mirrors.txt precise main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt precise-updates main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt precise-backports main restricted universe multiverse
deb mirror://mirrors.ubuntu.com/mirrors.txt precise-security main restricted universe multiverse

deb-src mirror://mirrors.ubuntu.com/mirrors.txt precise main restricted universe multiverse
deb-src mirror://mirrors.ubuntu.com/mirrors.txt precise-updates main restricted universe multiverse
deb-src mirror://mirrors.ubuntu.com/mirrors.txt precise-backports main restricted universe multiverse
deb-src mirror://mirrors.ubuntu.com/mirrors.txt precise-security main restricted universe multiverse
EOF
# aptitude update -q
# aptitude safe-upgrade -q -y
SCRIPT

Vagrant::Config.run do |config|
  config.vm.box = 'precise64'
  config.vm.box_url = 'http://files.vagrantup.com/precise64.box'
  config.ssh.forward_agent = true
end

Vagrant.configure("2") do |config|
  config.vm.provider :virtualbox do |vb, override|
    override.vm.provision :shell, inline: $script
    vb.customize ['modifyvm', :id, '--natdnshostresolver1', 'on']
    vb.customize ['modifyvm', :id, '--natdnsproxy1', 'on']
    vb.memory = 1024
  end
end
